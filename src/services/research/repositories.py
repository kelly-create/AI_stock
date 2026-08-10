"""Lease-fenced repositories for immutable research snapshots."""

from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import math
import re
from typing import Any, Mapping, Optional, Sequence
from urllib.parse import urlsplit, urlunsplit

from sqlalchemy import or_, select

from src.storage import (
    AnalysisJobRecord,
    DatabaseManager,
    JobEventRecord,
    ResearchDatasetSnapshotRecord,
    ResearchDebateRequestRecord,
    ResearchDebateSnapshotRecord,
    ResearchDebateTurnRecord,
    ResearchEvidenceSnapshotRecord,
    ResearchFactorSnapshotRecord,
    ResearchSnapshotRecord,
    to_utc_naive_datetime,
    utc_naive_now,
)

from .canonical import canonical_hash, canonical_json, canonicalize
from .datasets import normalize_status, validate_dataset_payload
from .debate_security import (
    strict_error_code,
    strict_public_identifier,
    strict_version_identifier,
)
from .factor_contract import validate_factor_contract


_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_RESEARCH_REFERENCE_EVENT_TYPE = "research_reference_time"
_DEBATE_STANCES = frozenset({"bull", "bear"})
_DEBATE_STATUSES = frozenset(
    {"available", "partial", "empty", "generation_failed"}
)


class ResearchReferenceTimeError(RuntimeError):
    """Raised when a durable job exposes an ambiguous research boundary."""


class ResearchReferenceTimeConflictError(ResearchReferenceTimeError):
    """Raised when a caller tries to replace an already frozen boundary."""

    def __init__(self, reference_time: datetime) -> None:
        self.reference_time = reference_time
        super().__init__(
            "research reference time is already frozen at "
            f"{reference_time.isoformat()}"
        )


@dataclass(frozen=True)
class LeaseFence:
    job_id: str
    worker_id: str
    lease_token: str


@dataclass(frozen=True)
class SnapshotWriteResult:
    record_id: int
    content_hash: str
    created: bool


@dataclass(frozen=True)
class DatasetSnapshotInput:
    dataset: str
    scope_type: str
    scope_value: str
    market: str
    provider: str
    schema_version: str
    data_as_of: datetime
    available_at: datetime
    observed_at: datetime
    status: str
    normalized: Any
    trade_date: Optional[date] = None
    report_date: Optional[date] = None
    announcement_date: Optional[date] = None
    raw_ref: Optional[Mapping[str, Any]] = None
    error_code: Optional[str] = None
    error_message_sanitized: Optional[str] = None
    supersedes_hash: Optional[str] = None
    knowledge_as_of: Optional[datetime] = None
    retryable: bool = False


@dataclass(frozen=True)
class FactorSnapshotInput:
    stock_code: str
    market: str
    company_profile: str
    engine_bundle_version: str
    factor_payload: Any
    input_dataset_hashes: Sequence[str]
    status: str
    coverage: float
    unknowns: Any
    as_of: datetime
    available_at: datetime
    primary_horizon: int = 10
    value_score: Optional[float] = None
    quality_score: Optional[float] = None
    trend_score: Optional[float] = None
    catalyst_score: Optional[float] = None
    risk_penalty: Optional[float] = None


@dataclass(frozen=True)
class EvidenceSnapshotInput:
    stock_code: str
    market: str
    evidence_engine_version: str
    claim_policy_version: str
    as_of: datetime
    available_at: datetime
    status: str
    coverage: float
    claim_count: int
    citation_count: int
    canonical_payload: Any
    input_dataset_hashes: Sequence[str]
    factor_snapshot_hash: str


@dataclass(frozen=True)
class DebateRequestInput:
    stock_code: str
    market: str
    debate_engine_version: str
    output_schema_version: str
    prompt_version: str
    as_of: datetime
    available_at: datetime
    evidence_snapshot_hash: str
    model_route_fingerprint: str
    canonical_payload: Any


@dataclass(frozen=True)
class DebateTurnInput:
    stock_code: str
    market: str
    stance: str
    debate_engine_version: str
    output_schema_version: str
    prompt_version: str
    as_of: datetime
    available_at: datetime
    evidence_snapshot_hash: str
    request_hash: str
    prompt_fingerprint: str
    model_route_fingerprint: str
    model_used: str
    canonical_payload: Any
    round_no: int = 1


@dataclass(frozen=True)
class DebateFailureInput:
    stock_code: str
    market: str
    stance: str
    debate_engine_version: str
    output_schema_version: str
    prompt_version: str
    as_of: datetime
    available_at: datetime
    evidence_snapshot_hash: str
    request_hash: str
    prompt_fingerprint: str
    model_route_fingerprint: str
    error_code: str


@dataclass(frozen=True)
class DebateSnapshotInput:
    stock_code: str
    market: str
    debate_engine_version: str
    output_schema_version: str
    prompt_version: str
    as_of: datetime
    available_at: datetime
    status: str
    evidence_snapshot_hash: str
    request_hash: str
    model_route_fingerprint: str
    bull_turn_hash: Optional[str]
    bear_turn_hash: Optional[str]
    bull_argument_count: int
    bear_argument_count: int
    open_question_count: int
    canonical_payload: Any


@dataclass(frozen=True)
class ResearchSnapshotInput:
    stock_code: str
    market: str
    snapshot_version: str
    field_dictionary_version: str
    factor_engine_version: str
    pack_version: str
    prompt_version: str
    policy_version: str
    model_route_fingerprint: str
    as_of: datetime
    available_at: datetime
    status: str
    canonical_payload: Any
    factor_snapshot_hash: Optional[str] = None
    evidence_snapshot_hash: Optional[str] = None
    debate_snapshot_hash: Optional[str] = None


def _required_text(value: Any, field_name: str, *, max_length: int) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    if len(normalized) > max_length:
        raise ValueError(f"{field_name} exceeds {max_length} characters")
    return normalized


def _market_identity(value: Any) -> str:
    """Normalize the two established A-share labels for cross-row checks."""

    normalized = str(value or "").strip().casefold()
    return "cn" if normalized in {"a", "cn"} else normalized


def _optional_text(value: Any, *, max_length: int) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    return normalized[:max_length]


_URL_IN_TEXT_PATTERN = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)
_SENSITIVE_URI_PATTERN = re.compile(
    r"(?i)\b(?:urn|data|file):[^\s<>\"']+"
)
_AUTHORIZATION_FIELD_PATTERN = re.compile(
    r"(?ix)\b(?:proxy[-_\s]?)?authorization\b['\"]?"
    r"\s*(?::|=|\s)\s*"
    r"(?:'[^']*'|\"[^\"]*\"|.*$)"
)
_COOKIE_FIELD_PATTERN = re.compile(
    r"(?ix)\b(?:set[-_\s]?)?cookie\b['\"]?"
    r"\s*(?::|=)\s*"
    r"(?:'[^']*'|\"[^\"]*\"|.*$)"
)
_SECRET_ASSIGNMENT_PATTERN = re.compile(
    r"(?ix)\b(token|api[\s_-]?key|access[\s_-]?token|refresh[\s_-]?token|"
    r"password|passwd|secret|secret[\s_-]?key|"
    r"client[\s_-]?secret|private[\s_-]?key|credentials?)\b['\"]?"
    r"(\s*(?::|=|\s)\s*)"
    r"(?:'[^']*'|\"[^\"]*\"|(?:bearer\s+)?[^\s,;}\]]+)"
)
_BEARER_PATTERN = re.compile(r"(?i)\bbearer\s+[^\s,;]+")
_TOKEN_LIKE_PATTERN = re.compile(
    r"(?i)\b(?:sk-[a-z0-9_-]{16,}|xox[baprs]-[a-z0-9-]{16,}|"
    r"gh[pousr]_[a-z0-9_]{20,})\b"
)


def _sanitize_url_match(match: re.Match[str]) -> str:
    raw = match.group(0)
    trailing = ""
    while raw and raw[-1] in ".,;)]}":
        trailing = raw[-1] + trailing
        raw = raw[:-1]
    try:
        parsed = urlsplit(raw)
        hostname = parsed.hostname
        if not hostname:
            return "[REDACTED_URL]" + trailing
        host = f"[{hostname}]" if ":" in hostname and not hostname.startswith("[") else hostname
        try:
            port = parsed.port
        except ValueError:
            port = None
        netloc = f"{host}:{port}" if port is not None else host
        safe_path = "/[REDACTED_PATH]" if parsed.path not in ("", "/") else parsed.path
        return urlunsplit((parsed.scheme, netloc, safe_path, "", "")) + trailing
    except (TypeError, ValueError):
        return "[REDACTED_URL]" + trailing


def sanitize_error_message(value: Any) -> Optional[str]:
    """Return bounded diagnostics without credentials or URL secrets."""

    if value is None:
        return None
    normalized = " ".join(str(value).split())
    normalized = _AUTHORIZATION_FIELD_PATTERN.sub(
        "Authorization: [REDACTED]",
        normalized,
    )
    normalized = _COOKIE_FIELD_PATTERN.sub("Cookie: [REDACTED]", normalized)
    normalized = _SENSITIVE_URI_PATTERN.sub("[REDACTED_URL]", normalized)
    normalized = _URL_IN_TEXT_PATTERN.sub(_sanitize_url_match, normalized)
    normalized = _SECRET_ASSIGNMENT_PATTERN.sub(
        lambda match: f"{match.group(1)}{match.group(2)}[REDACTED]",
        normalized,
    )
    normalized = _BEARER_PATTERN.sub("Bearer [REDACTED]", normalized)
    normalized = _TOKEN_LIKE_PATTERN.sub("[REDACTED_TOKEN]", normalized)
    return normalized[:500] or None


def _utc_datetime(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    return to_utc_naive_datetime(value)


def _optional_date(value: Optional[date], field_name: str) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime) or not isinstance(value, date):
        raise TypeError(f"{field_name} must be a date")
    return value


def _sha256(value: Any, field_name: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _SHA256_PATTERN.fullmatch(normalized):
        raise ValueError(f"{field_name} must be a lowercase SHA-256 hex digest")
    return normalized


def _optional_sha256(value: Any, field_name: str) -> Optional[str]:
    return None if value is None else _sha256(value, field_name)


def _score(value: Optional[float], field_name: str) -> Optional[float]:
    if value is None:
        return None
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0 or normalized > 100:
        raise ValueError(f"{field_name} must be finite and between 0 and 100")
    return normalized


def _nonnegative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _utc_text(value: datetime) -> str:
    return value.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _event_utc_datetime(value: Any, field_name: str) -> datetime:
    text_value = str(value or "").strip()
    try:
        parsed = datetime.fromisoformat(text_value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"stored {field_name} is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"stored {field_name} must include a UTC offset")
    return to_utc_naive_datetime(parsed)


def _debate_stance(value: Any) -> str:
    stance = _required_text(value, "stance", max_length=16).lower()
    if stance not in _DEBATE_STANCES:
        raise ValueError("stance must be bull or bear")
    return stance


def _debate_status(value: Any) -> str:
    status = _required_text(value, "status", max_length=32).lower()
    if status not in _DEBATE_STATUSES:
        raise ValueError(
            "status must be available, partial, empty, or generation_failed"
        )
    return status


def _debate_error_code(value: Any) -> str:
    """Accept only the same bounded identifier used by Debate artifacts."""

    if not isinstance(value, str):
        raise ValueError("error_code must be a safe identifier")
    bounded = _required_text(value, "error_code", max_length=64)
    try:
        return strict_error_code(bounded, field="error_code")
    except (TypeError, ValueError) as exc:
        raise ValueError("error_code must be a safe identifier") from exc


def _research_version(value: Any, field_name: str) -> str:
    """Validate one durable/public Research contract version identifier."""

    try:
        return strict_version_identifier(value, field=field_name)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a safe identifier") from exc


def _research_public_identifier(value: Any, field_name: str) -> str:
    try:
        return strict_public_identifier(value, field=field_name)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a safe identifier") from exc


def _optional_research_public_identifier(
    value: Any,
    field_name: str,
    *,
    max_length: int = 128,
) -> Optional[str]:
    if value is None:
        return None
    bounded = _required_text(value, field_name, max_length=max_length)
    return _research_public_identifier(bounded, field_name)


def _optional_research_error_code(value: Any) -> Optional[str]:
    if value is None or not str(value).strip():
        return None
    try:
        return strict_error_code(value, field="error_code")
    except (TypeError, ValueError) as exc:
        raise ValueError("error_code must be a safe identifier") from exc


def _research_dataset_hashes(value: Any) -> tuple[str, ...]:
    """Extract the complete immutable Dataset lineage from a frozen projection."""

    if not isinstance(value, Mapping):
        raise ValueError("canonical_payload datasets must be an object")
    hashes: set[str] = set()
    for dataset_name, raw_item in value.items():
        _required_text(dataset_name, "dataset projection name", max_length=64)
        if not isinstance(raw_item, Mapping):
            raise ValueError("each frozen dataset projection must be an object")
        current_hash = raw_item.get("content_hash")
        raw_hashes = raw_item.get("content_hashes")
        item_hashes: set[str] = set()
        if current_hash is not None:
            item_hashes.add(_sha256(current_hash, "dataset content_hash"))
        if raw_hashes is not None:
            if isinstance(raw_hashes, (str, bytes, bytearray)) or not isinstance(
                raw_hashes,
                Sequence,
            ):
                raise ValueError("dataset content_hashes must be an array")
            item_hashes.update(
                _sha256(item, "dataset content_hash") for item in raw_hashes
            )
        if not item_hashes:
            raise ValueError(
                f"frozen dataset projection {dataset_name!r} has no content hash"
            )
        hashes.update(item_hashes)
    return tuple(sorted(hashes))


def _validate_typed_payload_fields(
    payload: Any,
    expected_fields: Mapping[str, Any],
    *,
    artifact_name: str,
) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        raise ValueError(f"{artifact_name} canonical_payload must be an object")
    mismatched_fields = sorted(
        field_name
        for field_name, expected_value in expected_fields.items()
        if payload.get(field_name) != expected_value
    )
    if mismatched_fields:
        raise ValueError(
            f"canonical_payload conflicts with typed {artifact_name} fields: "
            + ",".join(mismatched_fields)
        )
    return payload


def _encode_evidence_cursor(as_of: datetime, record_id: int) -> str:
    payload = canonical_json(
        [_utc_text(as_of), int(record_id)],
        exclude_volatile=False,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _decode_evidence_cursor(value: str) -> tuple[datetime, int]:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > 256:
        raise ValueError("cursor is invalid")
    try:
        padding = "=" * (-len(normalized) % 4)
        decoded = base64.b64decode(
            normalized + padding,
            altchars=b"-_",
            validate=True,
        ).decode("utf-8")
        payload = json.loads(decoded)
        if (
            not isinstance(payload, list)
            or len(payload) != 2
            or not isinstance(payload[0], str)
            or isinstance(payload[1], bool)
            or not isinstance(payload[1], int)
            or payload[1] <= 0
        ):
            raise ValueError
        parsed = datetime.fromisoformat(payload[0].replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError
        return to_utc_naive_datetime(parsed), payload[1]
    except (binascii.Error, UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("cursor is invalid") from exc


class ResearchSnapshotRepository:
    """Content-addressed snapshot writes fenced by the current durable lease."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None) -> None:
        self.db = db_manager or DatabaseManager.get_instance()

    def assert_live_lease(
        self,
        lease: LeaseFence,
        *,
        now: Optional[datetime] = None,
    ) -> None:
        """Fail closed when a prepared research lease is no longer current."""

        current = _utc_datetime(now or datetime.now(timezone.utc), "now")
        with self.db.get_session() as session:
            self._assert_live_lease(session, lease, current)

    @staticmethod
    def _declared_job_stocks(job: AnalysisJobRecord) -> set[str]:
        """Return the durable job's explicit stock scope, if it declares one."""

        try:
            payload = json.loads(job.payload_json)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("durable job payload is invalid JSON") from exc
        if not isinstance(payload, Mapping):
            raise ValueError("durable job payload must be an object")
        scoped_payload = payload.get("data", payload)
        if not isinstance(scoped_payload, Mapping):
            raise ValueError("durable job payload data must be an object")
        stocks: set[str] = set()
        payload_stock = scoped_payload.get("stock_code")
        if isinstance(payload_stock, str) and payload_stock.strip():
            stocks.add(payload_stock.strip().upper())
        payload_stocks = scoped_payload.get("stock_codes")
        if payload_stocks is not None:
            if isinstance(payload_stocks, (str, bytes, bytearray)) or not isinstance(
                payload_stocks,
                Sequence,
            ):
                raise ValueError("durable job stock_codes must be an array")
            for item in payload_stocks:
                if not isinstance(item, str) or not item.strip():
                    raise ValueError("durable job stock_codes are invalid")
                stocks.add(item.strip().upper())
        row_stock = str(job.stock_code or "").strip()
        if not stocks and row_stock and row_stock.casefold() != job.job_type.casefold():
            stocks.add(row_stock.upper())
        return stocks

    @staticmethod
    def _assert_live_lease(
        session,
        lease: LeaseFence,
        now: datetime,
        *,
        stock_code: Optional[str] = None,
    ) -> None:
        # Keep this import local to avoid coupling the durable job module back to
        # the research repository at module-import time, but bind the symbol for
        # every rejection branch (not only the missing-row branch).
        from src.services.durable_jobs import StaleLeaseError

        job_id = _optional_research_public_identifier(
            lease.job_id,
            "job_id",
            max_length=64,
        )
        if job_id is None:
            raise ValueError("job_id is required")
        worker_id = _required_text(lease.worker_id, "worker_id", max_length=128)
        lease_token = _required_text(lease.lease_token, "lease_token", max_length=64)
        live_job = session.execute(
            select(AnalysisJobRecord).where(
                AnalysisJobRecord.task_id == job_id,
                AnalysisJobRecord.status == "processing",
                AnalysisJobRecord.cancel_requested_at.is_(None),
                AnalysisJobRecord.lease_owner == worker_id,
                AnalysisJobRecord.lease_token == lease_token,
                AnalysisJobRecord.lease_expires_at.is_not(None),
                AnalysisJobRecord.lease_expires_at > now,
            )
        ).scalar_one_or_none()
        if live_job is None:
            raise StaleLeaseError(
                f"research snapshot write rejected for stale or cancelled job {job_id!r}"
            )
        if live_job.job_type not in {
            "research",
            "research-collector",
            "stock_analysis",
            "personal_research",
            "decision_outcomes_v2",
            "scheduled_analysis",
        }:
            raise StaleLeaseError(
                "research snapshot write rejected for an unrelated job type"
            )
        if stock_code is None:
            return
        requested_stock = _required_text(
            stock_code,
            "stock_code",
            max_length=128,
        ).upper()
        allowed_stocks = ResearchSnapshotRepository._declared_job_stocks(live_job)
        if allowed_stocks and requested_stock not in allowed_stocks:
            raise StaleLeaseError(
                "research snapshot write rejected outside the job stock scope"
            )

    @staticmethod
    def _reference_payload(scope_value: str, reference_time: datetime) -> dict[str, str]:
        return {
            "scope_type": "stock",
            "scope_value": _required_text(
                scope_value,
                "scope_value",
                max_length=128,
            ),
            "as_of": reference_time.isoformat(timespec="microseconds") + "Z",
        }

    @staticmethod
    def _parse_reference_time(value: Any) -> datetime:
        text_value = str(value or "").strip()
        if not text_value:
            raise ResearchReferenceTimeError(
                "research reference event is missing as_of"
            )
        try:
            parsed = datetime.fromisoformat(text_value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ResearchReferenceTimeError(
                "research reference event contains an invalid as_of"
            ) from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ResearchReferenceTimeError(
                "research reference event as_of must include a UTC offset"
            )
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)

    @classmethod
    def _read_reference_time(
        cls,
        session,
        *,
        lease: LeaseFence,
        scope_value: str,
    ) -> Optional[datetime]:
        normalized_scope = _required_text(
            scope_value,
            "scope_value",
            max_length=128,
        )
        rows = session.execute(
            select(JobEventRecord.payload_json)
            .where(
                JobEventRecord.job_id == lease.job_id,
                JobEventRecord.event_type == _RESEARCH_REFERENCE_EVENT_TYPE,
            )
            .order_by(JobEventRecord.id.asc())
        ).scalars().all()
        reference_times: set[datetime] = set()
        for payload_json in rows:
            try:
                payload = json.loads(payload_json)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ResearchReferenceTimeError(
                    "research reference event payload is invalid JSON"
                ) from exc
            if not isinstance(payload, Mapping):
                raise ResearchReferenceTimeError(
                    "research reference event payload must be an object"
                )
            if payload.get("scope_type") != "stock":
                raise ResearchReferenceTimeError(
                    "research reference event has an unsupported scope_type"
                )
            event_scope = str(payload.get("scope_value") or "").strip()
            if not event_scope:
                raise ResearchReferenceTimeError(
                    "research reference event is missing scope_value"
                )
            if event_scope != normalized_scope:
                continue
            reference_times.add(cls._parse_reference_time(payload.get("as_of")))
        if len(reference_times) > 1:
            raise ResearchReferenceTimeError(
                "durable job contains multiple research reference times for "
                f"stock {normalized_scope!r}"
            )
        return next(iter(reference_times), None)

    @classmethod
    def _ensure_reference_time(
        cls,
        session,
        *,
        lease: LeaseFence,
        scope_value: str,
        candidate: datetime,
        now: datetime,
    ) -> datetime:
        frozen_candidate = _utc_datetime(candidate, "reference_time")
        existing = cls._read_reference_time(
            session,
            lease=lease,
            scope_value=scope_value,
        )
        if existing is not None:
            if existing != frozen_candidate:
                raise ResearchReferenceTimeConflictError(
                    existing.replace(tzinfo=timezone.utc)
                )
            return existing
        session.add(
            JobEventRecord(
                job_id=lease.job_id,
                event_type=_RESEARCH_REFERENCE_EVENT_TYPE,
                stage="research_data",
                payload_json=canonical_json(
                    cls._reference_payload(scope_value, frozen_candidate)
                ),
                created_at=now,
            )
        )
        session.flush()
        return frozen_candidate

    def get_research_reference_time(
        self,
        *,
        scope_value: str,
        lease: LeaseFence,
        now: Optional[datetime] = None,
    ) -> Optional[datetime]:
        """Read one job/stock boundary while proving the caller still owns the lease."""

        requested_now = _utc_datetime(now, "now") if now is not None else None

        def _read(session) -> Optional[datetime]:
            current = requested_now or utc_naive_now()
            self._assert_live_lease(
                session,
                lease,
                current,
                stock_code=scope_value,
            )
            value = self._read_reference_time(
                session,
                lease=lease,
                scope_value=scope_value,
            )
            return value.replace(tzinfo=timezone.utc) if value is not None else None

        return self.db._run_write_transaction(
            "read durable research reference time",
            _read,
        )

    def establish_research_reference_time(
        self,
        *,
        scope_value: str,
        reference_time: datetime,
        lease: LeaseFence,
        now: Optional[datetime] = None,
    ) -> datetime:
        """Freeze an explicit replay boundary before any provider request."""

        requested_now = _utc_datetime(now, "now") if now is not None else None
        candidate = _utc_datetime(reference_time, "reference_time")

        def _write(session) -> datetime:
            current = requested_now or utc_naive_now()
            self._assert_live_lease(
                session,
                lease,
                current,
                stock_code=scope_value,
            )
            value = self._ensure_reference_time(
                session,
                lease=lease,
                scope_value=scope_value,
                candidate=candidate,
                now=current,
            )
            return value.replace(tzinfo=timezone.utc)

        return self.db._run_write_transaction(
            "establish durable research reference time",
            _write,
        )

    def write_dataset(
        self,
        snapshot: DatasetSnapshotInput,
        *,
        lease: LeaseFence,
        now: Optional[datetime] = None,
        establish_reference_for: Optional[str] = None,
    ) -> SnapshotWriteResult:
        dataset_name = _required_text(snapshot.dataset, "dataset", max_length=64)
        if not isinstance(snapshot.retryable, bool):
            raise TypeError("retryable must be a bool")
        status = validate_dataset_payload(snapshot.status, snapshot.normalized)
        if snapshot.retryable and status != "fetch_failed":
            raise ValueError(
                "retryable Dataset checkpoints must use fetch_failed status"
            )
        data_as_of = _utc_datetime(snapshot.data_as_of, "data_as_of")
        available_at = _utc_datetime(snapshot.available_at, "available_at")
        observed_at = _utc_datetime(snapshot.observed_at, "observed_at")
        if available_at > observed_at:
            raise ValueError("available_at cannot be after observed_at")
        knowledge_as_of = (
            _utc_datetime(snapshot.knowledge_as_of, "knowledge_as_of")
            if snapshot.knowledge_as_of is not None
            else (observed_at if dataset_name == "stock_basic" else available_at)
        )
        if available_at > knowledge_as_of:
            raise ValueError("available_at cannot be after knowledge_as_of")
        if data_as_of > knowledge_as_of:
            raise ValueError("data_as_of cannot be after knowledge_as_of")
        if dataset_name == "stock_basic" and observed_at > knowledge_as_of:
            raise ValueError(
                "stock_basic observed_at cannot be after knowledge_as_of"
            )
        normalized_payload = (
            None if snapshot.normalized is None else canonicalize(snapshot.normalized)
        )
        normalized_json = (
            None if normalized_payload is None else canonical_json(normalized_payload)
        )
        raw_ref_payload = (
            None if snapshot.raw_ref is None else canonicalize(snapshot.raw_ref)
        )
        raw_ref_json = (
            None if raw_ref_payload is None else canonical_json(raw_ref_payload)
        )
        values = {
            "dataset": dataset_name,
            "scope_type": _required_text(snapshot.scope_type, "scope_type", max_length=32),
            "scope_value": _required_text(snapshot.scope_value, "scope_value", max_length=128),
            "market": _required_text(snapshot.market, "market", max_length=16),
            "provider": _required_text(snapshot.provider, "provider", max_length=64),
            "schema_version": _research_version(
                snapshot.schema_version, "schema_version"
            ),
            "trade_date": _optional_date(snapshot.trade_date, "trade_date"),
            "report_date": _optional_date(snapshot.report_date, "report_date"),
            "announcement_date": _optional_date(
                snapshot.announcement_date, "announcement_date"
            ),
            "data_as_of": data_as_of,
            "available_at": available_at,
            "observed_at": observed_at,
            "status": status,
            "normalized_json": normalized_json,
            "raw_ref_json": raw_ref_json,
            "error_code": _optional_research_error_code(snapshot.error_code),
            "error_message_sanitized": sanitize_error_message(
                snapshot.error_message_sanitized
            ),
            "supersedes_hash": _optional_sha256(
                snapshot.supersedes_hash, "supersedes_hash"
            ),
        }
        # Provider wording is diagnostic only. Historical datasets also
        # converge across equivalent observations, but current-state
        # ``stock_basic`` must bind its observation time because historical
        # visibility is defined by that boundary.
        identity_values = {
            key: value
            for key, value in values.items()
            if key != "error_message_sanitized"
            and not (key == "observed_at" and dataset_name != "stock_basic")
        }
        content_hash = canonical_hash(
            {
                **identity_values,
                "normalized_json": normalized_payload,
                "raw_ref_json": raw_ref_payload,
            }
        )
        requested_now = _utc_datetime(now, "now") if now is not None else None

        def _write(session) -> SnapshotWriteResult:
            current = requested_now or utc_naive_now()
            self._assert_live_lease(
                session,
                lease,
                current,
                stock_code=(
                    values["scope_value"]
                    if values["scope_type"] == "stock"
                    else None
                ),
            )
            if establish_reference_for is not None:
                self._ensure_reference_time(
                    session,
                    lease=lease,
                    scope_value=establish_reference_for,
                    candidate=knowledge_as_of,
                    now=current,
                )
            existing = session.execute(
                select(ResearchDatasetSnapshotRecord).where(
                    ResearchDatasetSnapshotRecord.content_hash == content_hash
                )
            ).scalar_one_or_none()
            if existing is not None:
                self._validate_stored_dataset_integrity(existing)
                self._bind_dataset_to_job(
                    session,
                    snapshot=snapshot,
                    lease=lease,
                    content_hash=content_hash,
                    knowledge_as_of=knowledge_as_of,
                    now=current,
                )
                return SnapshotWriteResult(int(existing.id), content_hash, False)
            row = ResearchDatasetSnapshotRecord(
                **values,
                content_hash=content_hash,
                origin_job_id=lease.job_id,
                created_at=current,
            )
            session.add(row)
            session.flush()
            self._bind_dataset_to_job(
                session,
                snapshot=snapshot,
                lease=lease,
                content_hash=content_hash,
                knowledge_as_of=knowledge_as_of,
                now=current,
            )
            return SnapshotWriteResult(int(row.id), content_hash, True)

        return self.db._run_write_transaction("write research dataset snapshot", _write)

    @staticmethod
    def _bind_dataset_to_job(
        session,
        *,
        snapshot: DatasetSnapshotInput,
        lease: LeaseFence,
        content_hash: str,
        knowledge_as_of: datetime,
        now: datetime,
    ) -> None:
        """Atomically bind a content-addressed row to this job's frozen boundary."""

        boundary_text = knowledge_as_of.isoformat(timespec="microseconds") + "Z"
        dataset = _required_text(snapshot.dataset, "dataset", max_length=64)
        scope_type = _required_text(
            snapshot.scope_type, "scope_type", max_length=32
        )
        scope_value = _required_text(
            snapshot.scope_value, "scope_value", max_length=128
        )
        payload_json = canonical_json(
            {
                "dataset": dataset,
                "scope_type": scope_type,
                "scope_value": scope_value,
                "knowledge_as_of": boundary_text,
                "content_hash": content_hash,
                "status": normalize_status(snapshot.status),
                "retryable": snapshot.retryable,
            }
        )
        existing = session.execute(
            select(JobEventRecord.id).where(
                JobEventRecord.job_id == lease.job_id,
                JobEventRecord.event_type == "research_dataset_snapshot",
                JobEventRecord.payload_json == payload_json,
            )
        ).scalar_one_or_none()
        if existing is not None:
            return
        session.add(
            JobEventRecord(
                job_id=lease.job_id,
                event_type="research_dataset_snapshot",
                stage="research_data",
                payload_json=payload_json,
                created_at=now,
            )
        )

    def write_factors(
        self,
        snapshot: FactorSnapshotInput,
        *,
        lease: LeaseFence,
        now: Optional[datetime] = None,
    ) -> SnapshotWriteResult:
        status = normalize_status(snapshot.status)
        as_of = _utc_datetime(snapshot.as_of, "as_of")
        available_at = _utc_datetime(snapshot.available_at, "available_at")
        if available_at > as_of:
            raise ValueError("available_at cannot be after as_of")
        primary_horizon = int(snapshot.primary_horizon)
        if primary_horizon <= 0:
            raise ValueError("primary_horizon must be positive")
        coverage = float(snapshot.coverage)
        if not math.isfinite(coverage) or coverage < 0 or coverage > 1:
            raise ValueError("coverage must be finite and between 0 and 1")
        validated_factor = validate_factor_contract(
            factor_payload=snapshot.factor_payload,
            unknowns=snapshot.unknowns,
            input_dataset_hashes=snapshot.input_dataset_hashes,
            as_of=snapshot.as_of,
            available_at=snapshot.available_at,
            value_score=snapshot.value_score,
            quality_score=snapshot.quality_score,
            trend_score=snapshot.trend_score,
            catalyst_score=snapshot.catalyst_score,
            risk_penalty=snapshot.risk_penalty,
        )
        factor_payload = canonicalize(
            validated_factor.factor_payload,
            exclude_volatile=False,
        )
        unknowns = canonicalize(
            validated_factor.unknowns,
            exclude_volatile=False,
        )
        dataset_hashes = list(validated_factor.input_dataset_hashes)
        values = {
            "stock_code": _required_text(snapshot.stock_code, "stock_code", max_length=16),
            "market": _required_text(snapshot.market, "market", max_length=16),
            "company_profile": _required_text(
                snapshot.company_profile, "company_profile", max_length=32
            ),
            "primary_horizon": primary_horizon,
            "engine_bundle_version": _research_version(
                snapshot.engine_bundle_version,
                "engine_bundle_version",
            ),
            "value_score": validated_factor.value_score,
            "quality_score": validated_factor.quality_score,
            "trend_score": validated_factor.trend_score,
            "catalyst_score": validated_factor.catalyst_score,
            "risk_penalty": validated_factor.risk_penalty,
            "factor_json": canonical_json(factor_payload),
            "input_dataset_hashes_json": canonical_json(dataset_hashes),
            "status": status,
            "coverage": coverage,
            "unknowns_json": canonical_json(unknowns),
            "as_of": as_of,
            "available_at": available_at,
        }
        content_hash = canonical_hash(
            {
                **values,
                "factor_json": factor_payload,
                "input_dataset_hashes_json": dataset_hashes,
                "unknowns_json": unknowns,
            }
        )
        requested_now = _utc_datetime(now, "now") if now is not None else None

        def _write(session) -> SnapshotWriteResult:
            current = requested_now or utc_naive_now()
            self._assert_live_lease(
                session,
                lease,
                current,
                stock_code=values["stock_code"],
            )
            stored_datasets = self._validate_stored_dataset_sources(
                session,
                stock_code=values["stock_code"],
                market=values["market"],
                as_of=as_of,
                dataset_hashes=dataset_hashes,
            )
            self._require_job_dataset_bindings(
                session,
                job_id=lease.job_id,
                datasets=stored_datasets,
                consumer_as_of=as_of,
                require_exact_hash=True,
            )
            existing = session.execute(
                select(ResearchFactorSnapshotRecord).where(
                    ResearchFactorSnapshotRecord.content_hash == content_hash
                )
            ).scalar_one_or_none()
            if existing is not None:
                self._validate_stored_factor_integrity(existing)
                self._bind_artifact_to_job(
                    session,
                    lease=lease,
                    event_type="research_factor_snapshot",
                    stage="research_factors",
                    payload={
                        "stock_code": values["stock_code"],
                        "as_of": as_of.isoformat(timespec="microseconds") + "Z",
                        "content_hash": content_hash,
                        "status": status,
                    },
                    now=current,
                )
                return SnapshotWriteResult(int(existing.id), content_hash, False)
            row = ResearchFactorSnapshotRecord(
                **values,
                content_hash=content_hash,
                origin_job_id=lease.job_id,
                created_at=current,
            )
            session.add(row)
            session.flush()
            self._bind_artifact_to_job(
                session,
                lease=lease,
                event_type="research_factor_snapshot",
                stage="research_factors",
                payload={
                    "stock_code": values["stock_code"],
                    "as_of": as_of.isoformat(timespec="microseconds") + "Z",
                    "content_hash": content_hash,
                    "status": status,
                },
                now=current,
            )
            return SnapshotWriteResult(int(row.id), content_hash, True)

        return self.db._run_write_transaction("write research factor snapshot", _write)

    def write_evidence(
        self,
        snapshot: EvidenceSnapshotInput,
        *,
        lease: LeaseFence,
        now: Optional[datetime] = None,
    ) -> SnapshotWriteResult:
        """Persist one immutable evidence graph after validating every source."""

        stock_code = _required_text(
            snapshot.stock_code,
            "stock_code",
            max_length=16,
        )
        market = _required_text(snapshot.market, "market", max_length=16)
        evidence_engine_version = _research_version(
            snapshot.evidence_engine_version,
            "evidence_engine_version",
        )
        claim_policy_version = _research_version(
            snapshot.claim_policy_version,
            "claim_policy_version",
        )
        as_of = _utc_datetime(snapshot.as_of, "as_of")
        available_at = _utc_datetime(snapshot.available_at, "available_at")
        if available_at > as_of:
            raise ValueError("available_at cannot be after as_of")
        status = normalize_status(snapshot.status)
        coverage = float(snapshot.coverage)
        if not math.isfinite(coverage) or coverage < 0 or coverage > 1:
            raise ValueError("coverage must be finite and between 0 and 1")
        claim_count = _nonnegative_int(snapshot.claim_count, "claim_count")
        citation_count = _nonnegative_int(snapshot.citation_count, "citation_count")
        factor_snapshot_hash = _sha256(
            snapshot.factor_snapshot_hash,
            "factor_snapshot_hash",
        )
        dataset_hashes = sorted(
            {
                _sha256(value, "input_dataset_hash")
                for value in snapshot.input_dataset_hashes
            }
        )
        evidence_payload = canonicalize(
            snapshot.canonical_payload,
            exclude_volatile=False,
        )
        if not isinstance(evidence_payload, Mapping):
            raise ValueError("canonical_payload must be an object")
        claims = evidence_payload.get("claims")
        citations = evidence_payload.get("citations")
        if not isinstance(claims, list) or len(claims) != claim_count:
            raise ValueError("claim_count must match canonical_payload claims")
        if not isinstance(citations, list) or len(citations) != citation_count:
            raise ValueError("citation_count must match canonical_payload citations")
        expected_payload_fields = {
            "stock_code": stock_code,
            "market": market,
            "evidence_engine_version": evidence_engine_version,
            "claim_policy_version": claim_policy_version,
            "as_of": _utc_text(as_of),
            "available_at": _utc_text(available_at),
            "status": status,
            "coverage": coverage,
            "input_dataset_hashes": dataset_hashes,
            "factor_snapshot_hash": factor_snapshot_hash,
        }
        mismatched_fields = sorted(
            field_name
            for field_name, expected_value in expected_payload_fields.items()
            if evidence_payload.get(field_name) != expected_value
        )
        if mismatched_fields:
            raise ValueError(
                "canonical_payload conflicts with typed evidence fields: "
                + ",".join(mismatched_fields)
            )
        values = {
            "stock_code": stock_code,
            "market": market,
            "evidence_engine_version": evidence_engine_version,
            "claim_policy_version": claim_policy_version,
            "as_of": as_of,
            "available_at": available_at,
            "status": status,
            "coverage": coverage,
            "claim_count": claim_count,
            "citation_count": citation_count,
            "canonical_json": canonical_json(
                evidence_payload,
                exclude_volatile=False,
            ),
            "input_dataset_hashes_json": canonical_json(
                dataset_hashes,
                exclude_volatile=False,
            ),
            "factor_snapshot_hash": factor_snapshot_hash,
        }
        evidence_hash = canonical_hash(
            evidence_payload,
            exclude_volatile=False,
        )
        validation_record = {
            **values,
            "evidence": evidence_payload,
            "evidence_hash": evidence_hash,
            "claim_count": claim_count,
            "citation_count": citation_count,
            "input_dataset_hashes": dataset_hashes,
        }
        from .evidence_service import hydrate_evidence_snapshot

        # The persistence boundary independently rehydrates the typed domain
        # graph. This rejects malformed shapes, dangling claim/citation edges,
        # duplicate identifiers, invalid statuses, and count/hash drift even
        # when a caller bypasses the normal evidence builder.
        hydrate_evidence_snapshot(validation_record)
        requested_now = _utc_datetime(now, "now") if now is not None else None

        def _write(session) -> SnapshotWriteResult:
            current = requested_now or utc_naive_now()
            self._assert_live_lease(
                session,
                lease,
                current,
                stock_code=stock_code,
            )
            factor, factor_datasets = self._validate_stored_factor_source(
                session,
                stock_code=stock_code,
                market=market,
                as_of=as_of,
                factor_snapshot_hash=factor_snapshot_hash,
            )
            evidence_datasets = self._validate_stored_dataset_sources(
                session,
                stock_code=stock_code,
                market=market,
                as_of=as_of,
                dataset_hashes=dataset_hashes,
            )
            self._require_job_factor_binding(
                session,
                job_id=lease.job_id,
                factor=factor,
            )
            factor_lineage = {row.content_hash for row in factor_datasets}
            direct_datasets = tuple(
                row
                for row in evidence_datasets
                if row.content_hash not in factor_lineage
            )
            self._require_job_dataset_bindings(
                session,
                job_id=lease.job_id,
                datasets=direct_datasets,
                consumer_as_of=as_of,
                require_exact_hash=True,
            )
            artifacts = self._validate_evidence_sources(
                session,
                stock_code=stock_code,
                market=market,
                as_of=as_of,
                factor_snapshot_hash=factor_snapshot_hash,
                dataset_hashes=dataset_hashes,
            )
            # With persisted artifacts available, the same domain validator
            # also checks citation JSON pointers, value hashes, source
            # boundaries, and artifact types before any row can be published.
            hydrate_evidence_snapshot(validation_record, artifacts=artifacts)
            existing = session.execute(
                select(ResearchEvidenceSnapshotRecord).where(
                    ResearchEvidenceSnapshotRecord.evidence_hash == evidence_hash
                )
            ).scalar_one_or_none()
            event_payload = {
                "stock_code": stock_code,
                "as_of": _utc_text(as_of),
                "evidence_hash": evidence_hash,
                "status": status,
            }
            for bound in self._job_artifact_event_payloads(
                session,
                job_id=lease.job_id,
                event_type="research_evidence_snapshot",
            ):
                if bound.get("stock_code") != stock_code:
                    continue
                bound_contract = {
                    "stock_code": stock_code,
                    "as_of": _utc_text(
                        _event_utc_datetime(
                            bound.get("as_of"),
                            "evidence binding as_of",
                        )
                    ),
                    "evidence_hash": _sha256(
                        bound.get("evidence_hash"), "evidence_hash"
                    ),
                    "status": normalize_status(bound.get("status")),
                }
                if bound_contract != event_payload:
                    raise ValueError(
                        "current job binds a conflicting Evidence snapshot contract"
                    )
            if existing is not None:
                self._validate_stored_evidence_for_debate(
                    session,
                    stock_code=stock_code,
                    market=market,
                    as_of=as_of,
                    evidence_snapshot_hash=evidence_hash,
                )
                self._bind_artifact_to_job(
                    session,
                    lease=lease,
                    event_type="research_evidence_snapshot",
                    stage="research_evidence",
                    payload=event_payload,
                    now=current,
                )
                return SnapshotWriteResult(int(existing.id), evidence_hash, False)
            row = ResearchEvidenceSnapshotRecord(
                **values,
                evidence_hash=evidence_hash,
                origin_job_id=lease.job_id,
                created_at=current,
            )
            session.add(row)
            session.flush()
            self._bind_artifact_to_job(
                session,
                lease=lease,
                event_type="research_evidence_snapshot",
                stage="research_evidence",
                payload=event_payload,
                now=current,
            )
            return SnapshotWriteResult(int(row.id), evidence_hash, True)

        return self.db._run_write_transaction(
            "write research evidence snapshot",
            _write,
        )

    @staticmethod
    def _validate_stored_dataset_integrity(
        row: ResearchDatasetSnapshotRecord,
    ) -> None:
        """Recompute one Dataset identity before any read or downstream use."""

        normalized_payload = _json_value(row.normalized_json)
        raw_ref_payload = _json_value(row.raw_ref_json)
        status = validate_dataset_payload(row.status, normalized_payload)
        if row.available_at > row.observed_at:
            raise ValueError("stored dataset available_at is after observed_at")
        values = {
            "dataset": _required_text(row.dataset, "dataset", max_length=64),
            "scope_type": _required_text(
                row.scope_type, "scope_type", max_length=32
            ),
            "scope_value": _required_text(
                row.scope_value, "scope_value", max_length=128
            ),
            "market": _required_text(row.market, "market", max_length=16),
            "provider": _required_text(row.provider, "provider", max_length=64),
            "schema_version": _research_version(
                row.schema_version, "schema_version"
            ),
            "trade_date": _optional_date(row.trade_date, "trade_date"),
            "report_date": _optional_date(row.report_date, "report_date"),
            "announcement_date": _optional_date(
                row.announcement_date, "announcement_date"
            ),
            "data_as_of": row.data_as_of,
            "available_at": row.available_at,
            "status": status,
            "normalized_json": normalized_payload,
            "raw_ref_json": raw_ref_payload,
            "error_code": _optional_research_error_code(row.error_code),
            "supersedes_hash": _optional_sha256(
                row.supersedes_hash, "supersedes_hash"
            ),
        }
        if row.dataset == "stock_basic":
            values["observed_at"] = row.observed_at
        expected_hash = canonical_hash(values)
        if _sha256(row.content_hash, "content_hash") != expected_hash:
            raise ValueError("stored dataset payload conflicts with content_hash")

    @staticmethod
    def _validate_stored_factor_integrity(
        row: ResearchFactorSnapshotRecord,
    ) -> tuple[str, ...]:
        """Recompute one Factor identity before any read or downstream use."""

        factor_payload = _json_value(row.factor_json)
        unknowns = _json_value(row.unknowns_json)
        raw_dataset_hashes = _json_value(row.input_dataset_hashes_json)
        validated_factor = validate_factor_contract(
            factor_payload=factor_payload,
            unknowns=unknowns,
            input_dataset_hashes=raw_dataset_hashes,
            as_of=row.as_of,
            available_at=row.available_at,
            value_score=row.value_score,
            quality_score=row.quality_score,
            trend_score=row.trend_score,
            catalyst_score=row.catalyst_score,
            risk_penalty=row.risk_penalty,
            require_canonical_references=True,
        )
        factor_payload = canonicalize(
            validated_factor.factor_payload,
            exclude_volatile=False,
        )
        unknowns = canonicalize(
            validated_factor.unknowns,
            exclude_volatile=False,
        )
        dataset_hashes = validated_factor.input_dataset_hashes
        values = {
            "stock_code": _required_text(
                row.stock_code, "stock_code", max_length=16
            ),
            "market": _required_text(row.market, "market", max_length=16),
            "company_profile": _required_text(
                row.company_profile, "company_profile", max_length=32
            ),
            "primary_horizon": _nonnegative_int(
                row.primary_horizon, "primary_horizon"
            ),
            "engine_bundle_version": _research_version(
                row.engine_bundle_version, "engine_bundle_version"
            ),
            "value_score": validated_factor.value_score,
            "quality_score": validated_factor.quality_score,
            "trend_score": validated_factor.trend_score,
            "catalyst_score": validated_factor.catalyst_score,
            "risk_penalty": validated_factor.risk_penalty,
            "factor_json": factor_payload,
            "input_dataset_hashes_json": list(dataset_hashes),
            "status": normalize_status(row.status),
            "coverage": float(row.coverage),
            "unknowns_json": unknowns,
            "as_of": row.as_of,
            "available_at": row.available_at,
        }
        if values["primary_horizon"] <= 0:
            raise ValueError("stored factor primary_horizon must be positive")
        coverage = values["coverage"]
        if not math.isfinite(coverage) or coverage < 0 or coverage > 1:
            raise ValueError("stored factor coverage is invalid")
        expected_hash = canonical_hash(values)
        if _sha256(row.content_hash, "content_hash") != expected_hash:
            raise ValueError("stored factor payload conflicts with content_hash")
        return dataset_hashes

    @staticmethod
    def _validate_stored_dataset_sources(
        session,
        *,
        stock_code: str,
        market: str,
        as_of: datetime,
        dataset_hashes: Sequence[str],
    ) -> tuple[ResearchDatasetSnapshotRecord, ...]:
        normalized_hashes = tuple(
            sorted({_sha256(value, "input_dataset_hash") for value in dataset_hashes})
        )
        datasets = (
            session.execute(
                select(ResearchDatasetSnapshotRecord).where(
                    ResearchDatasetSnapshotRecord.content_hash.in_(
                        normalized_hashes
                    )
                )
            ).scalars().all()
            if normalized_hashes
            else []
        )
        datasets_by_hash = {row.content_hash: row for row in datasets}
        missing_hashes = sorted(set(normalized_hashes).difference(datasets_by_hash))
        if missing_hashes:
            raise ValueError(
                "input_dataset_hashes do not reference stored datasets: "
                + ",".join(missing_hashes)
            )
        ordered = tuple(datasets_by_hash[digest] for digest in normalized_hashes)
        for dataset in ordered:
            ResearchSnapshotRepository._validate_stored_dataset_integrity(
                dataset
            )
            if (
                dataset.scope_type != "stock"
                or dataset.scope_value != stock_code
                or _market_identity(dataset.market) != _market_identity(market)
            ):
                raise ValueError(
                    "input dataset belongs to a different stock or market: "
                    + dataset.content_hash
                )
            if (
                dataset.data_as_of > as_of
                or dataset.available_at > as_of
                or (
                    dataset.dataset == "stock_basic"
                    and dataset.observed_at > as_of
                )
            ):
                raise ValueError(
                    "input dataset is after consumer as_of: "
                    + dataset.content_hash
                )
        return ordered

    @staticmethod
    def _validate_stored_factor_source(
        session,
        *,
        stock_code: str,
        market: str,
        as_of: datetime,
        factor_snapshot_hash: str,
    ) -> tuple[
        ResearchFactorSnapshotRecord,
        tuple[ResearchDatasetSnapshotRecord, ...],
    ]:
        factor = session.execute(
            select(ResearchFactorSnapshotRecord).where(
                ResearchFactorSnapshotRecord.content_hash
                == factor_snapshot_hash
            )
        ).scalar_one_or_none()
        if factor is None:
            raise ValueError("factor_snapshot_hash does not reference stored factors")
        factor_lineage = ResearchSnapshotRepository._validate_stored_factor_integrity(
            factor
        )
        if (
            factor.stock_code != stock_code
            or _market_identity(factor.market) != _market_identity(market)
        ):
            raise ValueError(
                "factor_snapshot_hash belongs to a different stock or market"
            )
        if factor.as_of > as_of or factor.available_at > as_of:
            raise ValueError("factor_snapshot_hash is after consumer as_of")
        datasets = ResearchSnapshotRepository._validate_stored_dataset_sources(
            session,
            stock_code=factor.stock_code,
            market=factor.market,
            as_of=factor.as_of,
            dataset_hashes=factor_lineage,
        )
        return factor, datasets

    @staticmethod
    def _validate_evidence_sources(
        session,
        *,
        stock_code: str,
        market: str,
        as_of: datetime,
        factor_snapshot_hash: str,
        dataset_hashes: Sequence[str],
    ) -> tuple[Any, ...]:
        from .evidence_service import EvidenceArtifact

        factor, _ = ResearchSnapshotRepository._validate_stored_factor_source(
            session,
            stock_code=stock_code,
            market=market,
            as_of=as_of,
            factor_snapshot_hash=factor_snapshot_hash,
        )

        datasets = ResearchSnapshotRepository._validate_stored_dataset_sources(
            session,
            stock_code=stock_code,
            market=market,
            as_of=as_of,
            dataset_hashes=dataset_hashes,
        )
        factor_lineage = _json_value(factor.input_dataset_hashes_json)
        artifacts = [
            EvidenceArtifact(
                artifact_type="dataset",
                artifact_hash=dataset.content_hash,
                stock_code=dataset.scope_value,
                available_at=dataset.available_at.replace(tzinfo=timezone.utc),
                payload=_json_value(dataset.normalized_json),
                source_name="",
            )
            for dataset in datasets
        ]
        artifacts.append(
            EvidenceArtifact(
                artifact_type="factor",
                artifact_hash=factor.content_hash,
                stock_code=factor.stock_code,
                available_at=factor.available_at.replace(tzinfo=timezone.utc),
                payload=_json_value(factor.factor_json),
                lineage_hashes=tuple(factor_lineage),
                source_name="deterministic_factor_engine",
            )
        )
        return tuple(artifacts)

    @staticmethod
    def _validate_stored_evidence_for_debate(
        session,
        *,
        stock_code: str,
        market: str,
        as_of: datetime,
        evidence_snapshot_hash: str,
    ) -> tuple[ResearchEvidenceSnapshotRecord, Any]:
        """Rehydrate the referenced DB evidence before publishing debate data."""

        evidence = session.execute(
            select(ResearchEvidenceSnapshotRecord).where(
                ResearchEvidenceSnapshotRecord.evidence_hash
                == evidence_snapshot_hash
            )
        ).scalar_one_or_none()
        if evidence is None:
            raise ValueError(
                "evidence_snapshot_hash does not reference stored evidence"
            )
        if (
            evidence.stock_code != stock_code
            or _market_identity(evidence.market) != _market_identity(market)
        ):
            raise ValueError(
                "evidence_snapshot_hash belongs to a different stock or market"
            )
        if evidence.as_of > as_of or evidence.available_at > as_of:
            raise ValueError("evidence_snapshot_hash is after debate as_of")

        from .evidence_service import hydrate_evidence_snapshot

        dataset_hashes = _json_value(evidence.input_dataset_hashes_json)
        if not isinstance(dataset_hashes, list):
            raise ValueError("stored evidence input_dataset_hashes are invalid")
        normalized_dataset_hashes = [
            _sha256(value, "input_dataset_hash") for value in dataset_hashes
        ]
        artifacts = ResearchSnapshotRepository._validate_evidence_sources(
            session,
            stock_code=evidence.stock_code,
            market=evidence.market,
            as_of=evidence.as_of,
            factor_snapshot_hash=_sha256(
                evidence.factor_snapshot_hash,
                "factor_snapshot_hash",
            ),
            dataset_hashes=normalized_dataset_hashes,
        )
        hydrated = hydrate_evidence_snapshot(
            _evidence_record_dict(evidence),
            artifacts=artifacts,
        )
        if hydrated.evidence_hash != evidence_snapshot_hash:
            raise ValueError("stored evidence payload conflicts with evidence_hash")
        return evidence, hydrated

    @classmethod
    def _validate_stored_debate_request(
        cls,
        session,
        *,
        request_hash: str,
        stock_code: str,
        market: str,
        debate_engine_version: str,
        output_schema_version: str,
        prompt_version: str,
        evidence_snapshot_hash: str,
        model_route_fingerprint: str,
        as_of: datetime,
    ) -> tuple[ResearchDebateRequestRecord, Any, Any]:
        request = session.execute(
            select(ResearchDebateRequestRecord).where(
                ResearchDebateRequestRecord.request_hash == request_hash
            )
        ).scalar_one_or_none()
        if request is None:
            raise ValueError("request_hash does not reference a stored debate request")
        expected_fields = {
            "stock_code": stock_code,
            "market": _market_identity(market),
            "debate_engine_version": debate_engine_version,
            "output_schema_version": output_schema_version,
            "prompt_version": prompt_version,
            "evidence_snapshot_hash": evidence_snapshot_hash,
            "model_route_fingerprint": model_route_fingerprint,
        }
        actual_fields = {
            "stock_code": request.stock_code,
            "market": _market_identity(request.market),
            "debate_engine_version": request.debate_engine_version,
            "output_schema_version": request.output_schema_version,
            "prompt_version": request.prompt_version,
            "evidence_snapshot_hash": request.evidence_snapshot_hash,
            "model_route_fingerprint": request.model_route_fingerprint,
        }
        if actual_fields != expected_fields:
            raise ValueError("request_hash conflicts with debate lineage")
        if request.as_of > as_of or request.available_at > as_of:
            raise ValueError("request_hash is after debate as_of")

        _, hydrated_evidence = cls._validate_stored_evidence_for_debate(
            session,
            stock_code=stock_code,
            market=market,
            as_of=request.as_of,
            evidence_snapshot_hash=evidence_snapshot_hash,
        )
        from .debate_service import hydrate_debate_request

        hydrated_request = hydrate_debate_request(
            _debate_request_record_dict(request),
            evidence_snapshot=hydrated_evidence,
        )
        if hydrated_request.request_hash != request_hash:
            raise ValueError("stored debate request payload conflicts with request_hash")
        return request, hydrated_request, hydrated_evidence

    def write_debate_request(
        self,
        snapshot: DebateRequestInput,
        *,
        lease: LeaseFence,
        now: Optional[datetime] = None,
    ) -> SnapshotWriteResult:
        """Freeze exact per-stance messages before either debate model call."""

        stock_code = _required_text(
            snapshot.stock_code,
            "stock_code",
            max_length=16,
        )
        market = _required_text(snapshot.market, "market", max_length=16)
        debate_engine_version = _research_version(
            snapshot.debate_engine_version,
            "debate_engine_version",
        )
        output_schema_version = _research_version(
            snapshot.output_schema_version,
            "output_schema_version",
        )
        prompt_version = _research_version(
            snapshot.prompt_version,
            "prompt_version",
        )
        evidence_snapshot_hash = _sha256(
            snapshot.evidence_snapshot_hash,
            "evidence_snapshot_hash",
        )
        model_route_fingerprint = _required_text(
            snapshot.model_route_fingerprint,
            "model_route_fingerprint",
            max_length=128,
        )
        as_of = _utc_datetime(snapshot.as_of, "as_of")
        available_at = _utc_datetime(snapshot.available_at, "available_at")
        if available_at > as_of:
            raise ValueError("available_at cannot be after as_of")
        request_payload = canonicalize(
            snapshot.canonical_payload,
            exclude_volatile=False,
        )
        _validate_typed_payload_fields(
            request_payload,
            {
                "stock_code": stock_code,
                "market": market,
                "debate_engine_version": debate_engine_version,
                "output_schema_version": output_schema_version,
                "prompt_version": prompt_version,
                "as_of": _utc_text(as_of),
                "available_at": _utc_text(available_at),
                "evidence_snapshot_hash": evidence_snapshot_hash,
                "model_route_fingerprint": model_route_fingerprint,
            },
            artifact_name="debate request",
        )
        values = {
            "stock_code": stock_code,
            "market": market,
            "debate_engine_version": debate_engine_version,
            "output_schema_version": output_schema_version,
            "prompt_version": prompt_version,
            "evidence_snapshot_hash": evidence_snapshot_hash,
            "model_route_fingerprint": model_route_fingerprint,
            "as_of": as_of,
            "available_at": available_at,
            "canonical_json": canonical_json(
                request_payload,
                exclude_volatile=False,
            ),
        }
        request_hash = canonical_hash(
            request_payload,
            exclude_volatile=False,
        )
        validation_record = {
            **values,
            "as_of": _utc_text(as_of),
            "available_at": _utc_text(available_at),
            "debate_request": request_payload,
            "request_hash": request_hash,
        }
        from .debate_service import hydrate_debate_request

        hydrate_debate_request(validation_record)
        requested_now = _utc_datetime(now, "now") if now is not None else None

        def _write(session) -> SnapshotWriteResult:
            current = requested_now or utc_naive_now()
            self._assert_live_lease(
                session,
                lease,
                current,
                stock_code=stock_code,
            )
            evidence_row, hydrated_evidence = (
                self._validate_stored_evidence_for_debate(
                    session,
                    stock_code=stock_code,
                    market=market,
                    as_of=as_of,
                    evidence_snapshot_hash=evidence_snapshot_hash,
                )
            )
            self._require_job_evidence_binding(
                session,
                job_id=lease.job_id,
                evidence=evidence_row,
            )
            hydrate_debate_request(
                validation_record,
                evidence_snapshot=hydrated_evidence,
            )
            request_bindings = self._job_artifact_event_payloads(
                session,
                job_id=lease.job_id,
                event_type="research_debate_request",
            )
            for payload in request_bindings:
                if (
                    payload.get("stock_code") != stock_code
                    or payload.get("evidence_snapshot_hash")
                    != evidence_snapshot_hash
                ):
                    continue
                bound_hash = _sha256(
                    payload.get("request_hash"),
                    "request_hash",
                )
                bound_as_of = _event_utc_datetime(
                    payload.get("as_of"),
                    "request binding as_of",
                )
                if (
                    bound_hash != request_hash
                    or bound_as_of != as_of
                    or payload.get("prompt_version") != prompt_version
                    or payload.get("model_route_fingerprint")
                    != model_route_fingerprint
                ):
                    raise ValueError(
                        "job already binds a conflicting Debate request contract"
                    )
            existing = session.execute(
                select(ResearchDebateRequestRecord).where(
                    ResearchDebateRequestRecord.request_hash == request_hash
                )
            ).scalar_one_or_none()
            event_payload = {
                "stock_code": stock_code,
                "as_of": _utc_text(as_of),
                "evidence_snapshot_hash": evidence_snapshot_hash,
                "prompt_version": prompt_version,
                "model_route_fingerprint": model_route_fingerprint,
                "request_hash": request_hash,
            }
            if existing is not None:
                hydrate_debate_request(
                    _debate_request_record_dict(existing),
                    evidence_snapshot=hydrated_evidence,
                )
                self._bind_artifact_to_job(
                    session,
                    lease=lease,
                    event_type="research_debate_request",
                    stage="research_debate",
                    payload=event_payload,
                    now=current,
                )
                return SnapshotWriteResult(int(existing.id), request_hash, False)
            row = ResearchDebateRequestRecord(
                **values,
                request_hash=request_hash,
                origin_job_id=lease.job_id,
                created_at=current,
            )
            session.add(row)
            session.flush()
            self._bind_artifact_to_job(
                session,
                lease=lease,
                event_type="research_debate_request",
                stage="research_debate",
                payload=event_payload,
                now=current,
            )
            return SnapshotWriteResult(int(row.id), request_hash, True)

        return self.db._run_write_transaction(
            "write research debate request",
            _write,
        )

    def write_debate_turn(
        self,
        snapshot: DebateTurnInput,
        *,
        lease: LeaseFence,
        now: Optional[datetime] = None,
    ) -> SnapshotWriteResult:
        """Persist one immutable bull/bear result against its frozen request."""

        stock_code = _required_text(
            snapshot.stock_code,
            "stock_code",
            max_length=16,
        )
        market = _required_text(snapshot.market, "market", max_length=16)
        stance = _debate_stance(snapshot.stance)
        if isinstance(snapshot.round_no, bool) or int(snapshot.round_no) != 1:
            raise ValueError("round_no must be 1")
        round_no = 1
        debate_engine_version = _research_version(
            snapshot.debate_engine_version,
            "debate_engine_version",
        )
        output_schema_version = _research_version(
            snapshot.output_schema_version,
            "output_schema_version",
        )
        prompt_version = _research_version(
            snapshot.prompt_version,
            "prompt_version",
        )
        evidence_snapshot_hash = _sha256(
            snapshot.evidence_snapshot_hash,
            "evidence_snapshot_hash",
        )
        request_hash = _sha256(snapshot.request_hash, "request_hash")
        prompt_fingerprint = _sha256(
            snapshot.prompt_fingerprint,
            "prompt_fingerprint",
        )
        model_route_fingerprint = _required_text(
            snapshot.model_route_fingerprint,
            "model_route_fingerprint",
            max_length=128,
        )
        model_used = _required_text(
            snapshot.model_used,
            "model_used",
            max_length=128,
        )
        as_of = _utc_datetime(snapshot.as_of, "as_of")
        available_at = _utc_datetime(snapshot.available_at, "available_at")
        if available_at > as_of:
            raise ValueError("available_at cannot be after as_of")
        turn_payload = canonicalize(
            snapshot.canonical_payload,
            exclude_volatile=False,
        )
        _validate_typed_payload_fields(
            turn_payload,
            {
                "stock_code": stock_code,
                "market": market,
                "stance": stance,
                "debate_engine_version": debate_engine_version,
                "output_schema_version": output_schema_version,
                "prompt_version": prompt_version,
                "as_of": _utc_text(as_of),
                "available_at": _utc_text(available_at),
                "evidence_snapshot_hash": evidence_snapshot_hash,
                "request_hash": request_hash,
                "prompt_fingerprint": prompt_fingerprint,
                "model_route_fingerprint": model_route_fingerprint,
                "model_used": model_used,
            },
            artifact_name="debate turn",
        )
        values = {
            "stock_code": stock_code,
            "market": market,
            "stance": stance,
            "round_no": round_no,
            "debate_engine_version": debate_engine_version,
            "output_schema_version": output_schema_version,
            "prompt_version": prompt_version,
            "evidence_snapshot_hash": evidence_snapshot_hash,
            "request_hash": request_hash,
            "prompt_fingerprint": prompt_fingerprint,
            "model_route_fingerprint": model_route_fingerprint,
            "model_used": model_used,
            "as_of": as_of,
            "available_at": available_at,
            "canonical_json": canonical_json(
                turn_payload,
                exclude_volatile=False,
            ),
        }
        turn_hash = canonical_hash(turn_payload, exclude_volatile=False)
        validation_record = {
            **values,
            "as_of": _utc_text(as_of),
            "available_at": _utc_text(available_at),
            "debate_turn": turn_payload,
            "turn_hash": turn_hash,
        }
        from .debate_service import hydrate_debate_turn

        hydrate_debate_turn(validation_record)
        requested_now = _utc_datetime(now, "now") if now is not None else None

        def _write(session) -> SnapshotWriteResult:
            current = requested_now or utc_naive_now()
            self._assert_live_lease(
                session,
                lease,
                current,
                stock_code=stock_code,
            )
            request, hydrated_request, _ = self._validate_stored_debate_request(
                session,
                request_hash=request_hash,
                stock_code=stock_code,
                market=market,
                debate_engine_version=debate_engine_version,
                output_schema_version=output_schema_version,
                prompt_version=prompt_version,
                evidence_snapshot_hash=evidence_snapshot_hash,
                model_route_fingerprint=model_route_fingerprint,
                as_of=as_of,
            )
            self._require_job_debate_request_binding(
                session,
                job_id=lease.job_id,
                stock_code=stock_code,
                evidence_snapshot_hash=evidence_snapshot_hash,
                request_hash=request_hash,
                prompt_version=prompt_version,
                model_route_fingerprint=model_route_fingerprint,
                as_of=request.as_of,
            )
            request_payload = hydrated_request.canonical_payload
            request_turns = request_payload.get("turn_requests")
            if isinstance(request_turns, (str, bytes, bytearray)) or not isinstance(
                request_turns,
                Sequence,
            ):
                raise ValueError("stored debate request turn_requests are invalid")
            matching_requests = [
                item
                for item in request_turns
                if isinstance(item, Mapping) and item.get("stance") == stance
            ]
            if len(matching_requests) != 1:
                raise ValueError("stored debate request stance is missing or ambiguous")
            if matching_requests[0].get("prompt_fingerprint") != prompt_fingerprint:
                raise ValueError(
                    "prompt_fingerprint conflicts with the stored debate request"
                )
            if request.available_at > available_at:
                raise ValueError("debate turn predates its frozen request")
            hydrate_debate_turn(
                validation_record,
                request=hydrated_request,
            )
            expected_prompt_fingerprints = {
                item.stance: item.prompt_fingerprint
                for item in hydrated_request.turn_requests
            }
            persisted_failures = self._validated_job_debate_failures(
                session,
                job_id=lease.job_id,
                stock_code=stock_code,
                evidence_snapshot_hash=evidence_snapshot_hash,
                request_hash=request_hash,
                prompt_version=prompt_version,
                prompt_fingerprints=expected_prompt_fingerprints,
                model_route_fingerprint=model_route_fingerprint,
                as_of=as_of,
                available_at=available_at,
            )
            if stance in persisted_failures:
                raise ValueError(
                    "Debate stance already has a persisted terminal failure"
                )
            persisted_turns = self._validated_job_debate_turn_hashes(
                session,
                job_id=lease.job_id,
                stock_code=stock_code,
                evidence_snapshot_hash=evidence_snapshot_hash,
                request_hash=request_hash,
                prompt_version=prompt_version,
                prompt_fingerprints=expected_prompt_fingerprints,
                model_route_fingerprint=model_route_fingerprint,
                as_of=as_of,
            )
            if stance in persisted_turns and persisted_turns[stance] != turn_hash:
                raise ValueError(
                    "job already binds a conflicting Debate turn for this stance"
                )

            existing = session.execute(
                select(ResearchDebateTurnRecord).where(
                    ResearchDebateTurnRecord.turn_hash == turn_hash
                )
            ).scalar_one_or_none()
            event_payload = {
                "stock_code": stock_code,
                "as_of": _utc_text(as_of),
                "stance": stance,
                "evidence_snapshot_hash": evidence_snapshot_hash,
                "request_hash": request_hash,
                "prompt_version": prompt_version,
                "prompt_fingerprint": prompt_fingerprint,
                "model_route_fingerprint": model_route_fingerprint,
                "turn_hash": turn_hash,
            }
            if existing is not None:
                hydrate_debate_turn(
                    _debate_turn_record_dict(existing),
                    request=hydrated_request,
                )
                self._bind_artifact_to_job(
                    session,
                    lease=lease,
                    event_type="research_debate_turn",
                    stage="research_debate",
                    payload=event_payload,
                    now=current,
                )
                return SnapshotWriteResult(int(existing.id), turn_hash, False)
            row = ResearchDebateTurnRecord(
                **values,
                turn_hash=turn_hash,
                origin_job_id=lease.job_id,
                created_at=current,
            )
            session.add(row)
            session.flush()
            self._bind_artifact_to_job(
                session,
                lease=lease,
                event_type="research_debate_turn",
                stage="research_debate",
                payload=event_payload,
                now=current,
            )
            return SnapshotWriteResult(int(row.id), turn_hash, True)

        return self.db._run_write_transaction(
            "write research debate turn",
            _write,
        )

    def write_debate_failure(
        self,
        failure: DebateFailureInput,
        *,
        lease: LeaseFence,
        now: Optional[datetime] = None,
    ) -> None:
        """Checkpoint one terminal stance failure under the active job lease."""

        stock_code = _required_text(
            failure.stock_code,
            "stock_code",
            max_length=16,
        )
        market = _required_text(failure.market, "market", max_length=16)
        stance = _debate_stance(failure.stance)
        debate_engine_version = _research_version(
            failure.debate_engine_version,
            "debate_engine_version",
        )
        output_schema_version = _research_version(
            failure.output_schema_version,
            "output_schema_version",
        )
        prompt_version = _research_version(
            failure.prompt_version,
            "prompt_version",
        )
        evidence_snapshot_hash = _sha256(
            failure.evidence_snapshot_hash,
            "evidence_snapshot_hash",
        )
        request_hash = _sha256(failure.request_hash, "request_hash")
        prompt_fingerprint = _sha256(
            failure.prompt_fingerprint,
            "prompt_fingerprint",
        )
        model_route_fingerprint = _required_text(
            failure.model_route_fingerprint,
            "model_route_fingerprint",
            max_length=128,
        )
        error_code = _debate_error_code(failure.error_code)
        as_of = _utc_datetime(failure.as_of, "as_of")
        available_at = _utc_datetime(failure.available_at, "available_at")
        if available_at > as_of:
            raise ValueError("available_at cannot be after as_of")
        requested_now = _utc_datetime(now, "now") if now is not None else None

        def _write(session) -> None:
            current = requested_now or utc_naive_now()
            self._assert_live_lease(
                session,
                lease,
                current,
                stock_code=stock_code,
            )
            request_row, hydrated_request, _ = self._validate_stored_debate_request(
                session,
                request_hash=request_hash,
                stock_code=stock_code,
                market=market,
                debate_engine_version=debate_engine_version,
                output_schema_version=output_schema_version,
                prompt_version=prompt_version,
                evidence_snapshot_hash=evidence_snapshot_hash,
                model_route_fingerprint=model_route_fingerprint,
                as_of=as_of,
            )
            self._require_job_debate_request_binding(
                session,
                job_id=lease.job_id,
                stock_code=stock_code,
                evidence_snapshot_hash=evidence_snapshot_hash,
                request_hash=request_hash,
                prompt_version=prompt_version,
                model_route_fingerprint=model_route_fingerprint,
                as_of=request_row.as_of,
            )
            if request_row.available_at != available_at:
                raise ValueError("debate failure boundary conflicts with its request")
            expected_prompt_fingerprints = {
                item.stance: item.prompt_fingerprint
                for item in hydrated_request.turn_requests
            }
            if expected_prompt_fingerprints.get(stance) != prompt_fingerprint:
                raise ValueError(
                    "debate failure prompt conflicts with its frozen request"
                )
            existing_failures = self._validated_job_debate_failures(
                session,
                job_id=lease.job_id,
                stock_code=stock_code,
                evidence_snapshot_hash=evidence_snapshot_hash,
                request_hash=request_hash,
                prompt_version=prompt_version,
                prompt_fingerprints=expected_prompt_fingerprints,
                model_route_fingerprint=model_route_fingerprint,
                as_of=as_of,
                available_at=available_at,
            )
            if stance in existing_failures:
                if existing_failures[stance]["error_code"] != error_code:
                    raise ValueError("debate failure binding is ambiguous")
                return
            existing_turns = self._validated_job_debate_turn_hashes(
                session,
                job_id=lease.job_id,
                stock_code=stock_code,
                evidence_snapshot_hash=evidence_snapshot_hash,
                request_hash=request_hash,
                prompt_version=prompt_version,
                prompt_fingerprints=expected_prompt_fingerprints,
                model_route_fingerprint=model_route_fingerprint,
                as_of=as_of,
            )
            if stance in existing_turns:
                raise ValueError(
                    "Debate stance already has a persisted successful turn"
                )
            self._bind_artifact_to_job(
                session,
                lease=lease,
                event_type="research_debate_failure",
                stage="research_debate",
                payload={
                    "stock_code": stock_code,
                    "as_of": _utc_text(as_of),
                    "available_at": _utc_text(available_at),
                    "stance": stance,
                    "evidence_snapshot_hash": evidence_snapshot_hash,
                    "request_hash": request_hash,
                    "prompt_version": prompt_version,
                    "prompt_fingerprint": prompt_fingerprint,
                    "model_route_fingerprint": model_route_fingerprint,
                    "error_code": error_code,
                },
                now=current,
            )

        self.db._run_write_transaction(
            "write research debate failure",
            _write,
        )

    def write_debate_snapshot(
        self,
        snapshot: DebateSnapshotInput,
        *,
        lease: LeaseFence,
        now: Optional[datetime] = None,
    ) -> SnapshotWriteResult:
        """Assemble an immutable debate only from fully revalidated DB turns."""

        stock_code = _required_text(
            snapshot.stock_code,
            "stock_code",
            max_length=16,
        )
        market = _required_text(snapshot.market, "market", max_length=16)
        debate_engine_version = _research_version(
            snapshot.debate_engine_version,
            "debate_engine_version",
        )
        output_schema_version = _research_version(
            snapshot.output_schema_version,
            "output_schema_version",
        )
        prompt_version = _research_version(
            snapshot.prompt_version,
            "prompt_version",
        )
        evidence_snapshot_hash = _sha256(
            snapshot.evidence_snapshot_hash,
            "evidence_snapshot_hash",
        )
        request_hash = _sha256(snapshot.request_hash, "request_hash")
        model_route_fingerprint = _required_text(
            snapshot.model_route_fingerprint,
            "model_route_fingerprint",
            max_length=128,
        )
        status = _debate_status(snapshot.status)
        bull_turn_hash = _optional_sha256(
            snapshot.bull_turn_hash,
            "bull_turn_hash",
        )
        bear_turn_hash = _optional_sha256(
            snapshot.bear_turn_hash,
            "bear_turn_hash",
        )
        if bull_turn_hash is not None and bull_turn_hash == bear_turn_hash:
            raise ValueError("bull_turn_hash and bear_turn_hash must be distinct")
        if status == "available" and (
            bull_turn_hash is None or bear_turn_hash is None
        ):
            raise ValueError("available debate requires both bull and bear turns")
        if status == "partial" and (
            (bull_turn_hash is None) == (bear_turn_hash is None)
        ):
            raise ValueError("partial debate requires exactly one turn")
        if status in {"empty", "generation_failed"} and (
            bull_turn_hash is not None or bear_turn_hash is not None
        ):
            raise ValueError(f"{status} debate cannot reference turns")
        bull_argument_count = _nonnegative_int(
            snapshot.bull_argument_count,
            "bull_argument_count",
        )
        bear_argument_count = _nonnegative_int(
            snapshot.bear_argument_count,
            "bear_argument_count",
        )
        open_question_count = _nonnegative_int(
            snapshot.open_question_count,
            "open_question_count",
        )
        as_of = _utc_datetime(snapshot.as_of, "as_of")
        available_at = _utc_datetime(snapshot.available_at, "available_at")
        if available_at > as_of:
            raise ValueError("available_at cannot be after as_of")
        debate_payload = canonicalize(
            snapshot.canonical_payload,
            exclude_volatile=False,
        )
        _validate_typed_payload_fields(
            debate_payload,
            {
                "stock_code": stock_code,
                "market": market,
                "debate_engine_version": debate_engine_version,
                "output_schema_version": output_schema_version,
                "prompt_version": prompt_version,
                "as_of": _utc_text(as_of),
                "available_at": _utc_text(available_at),
                "status": status,
                "evidence_snapshot_hash": evidence_snapshot_hash,
                "request_hash": request_hash,
                "model_route_fingerprint": model_route_fingerprint,
                "bull_turn_hash": bull_turn_hash,
                "bear_turn_hash": bear_turn_hash,
            },
            artifact_name="debate snapshot",
        )
        values = {
            "stock_code": stock_code,
            "market": market,
            "debate_engine_version": debate_engine_version,
            "output_schema_version": output_schema_version,
            "prompt_version": prompt_version,
            "evidence_snapshot_hash": evidence_snapshot_hash,
            "request_hash": request_hash,
            "model_route_fingerprint": model_route_fingerprint,
            "as_of": as_of,
            "available_at": available_at,
            "status": status,
            "bull_turn_hash": bull_turn_hash,
            "bear_turn_hash": bear_turn_hash,
            "bull_argument_count": bull_argument_count,
            "bear_argument_count": bear_argument_count,
            "open_question_count": open_question_count,
            "canonical_json": canonical_json(
                debate_payload,
                exclude_volatile=False,
            ),
        }
        debate_hash = canonical_hash(debate_payload, exclude_volatile=False)
        validation_record = {
            **values,
            "as_of": _utc_text(as_of),
            "available_at": _utc_text(available_at),
            "debate": debate_payload,
            "debate_hash": debate_hash,
        }
        from .debate_service import hydrate_debate_snapshot

        hydrate_debate_snapshot(validation_record)
        requested_now = _utc_datetime(now, "now") if now is not None else None

        def _write(session) -> SnapshotWriteResult:
            current = requested_now or utc_naive_now()
            self._assert_live_lease(
                session,
                lease,
                current,
                stock_code=values["stock_code"],
            )
            request, hydrated_request, _ = self._validate_stored_debate_request(
                session,
                request_hash=request_hash,
                stock_code=stock_code,
                market=market,
                debate_engine_version=debate_engine_version,
                output_schema_version=output_schema_version,
                prompt_version=prompt_version,
                evidence_snapshot_hash=evidence_snapshot_hash,
                model_route_fingerprint=model_route_fingerprint,
                as_of=as_of,
            )
            self._require_job_debate_request_binding(
                session,
                job_id=lease.job_id,
                stock_code=stock_code,
                evidence_snapshot_hash=evidence_snapshot_hash,
                request_hash=request_hash,
                prompt_version=prompt_version,
                model_route_fingerprint=model_route_fingerprint,
                as_of=request.as_of,
            )
            if request.available_at > available_at:
                raise ValueError("debate snapshot predates its frozen request")
            hydrate_debate_snapshot(
                validation_record,
                request=hydrated_request,
            )

            prompt_fingerprints = {
                item.stance: item.prompt_fingerprint
                for item in hydrated_request.turn_requests
            }
            persisted_failures = self._validated_job_debate_failures(
                session,
                job_id=lease.job_id,
                stock_code=stock_code,
                evidence_snapshot_hash=evidence_snapshot_hash,
                request_hash=request_hash,
                prompt_version=prompt_version,
                prompt_fingerprints=prompt_fingerprints,
                model_route_fingerprint=model_route_fingerprint,
                as_of=as_of,
                available_at=available_at,
            )
            expected_failures = [
                persisted_failures[stance]
                for stance in ("bull", "bear")
                if stance in persisted_failures
            ]
            if canonical_json(
                debate_payload.get("failed_stances"),
                exclude_volatile=False,
            ) != canonical_json(expected_failures, exclude_volatile=False):
                raise ValueError(
                    "debate snapshot failures conflict with durable stance checkpoints"
                )

            expected_bound_turns = {
                stance: digest
                for stance, digest in (
                    ("bull", bull_turn_hash),
                    ("bear", bear_turn_hash),
                )
                if digest is not None
            }
            persisted_turn_hashes = self._validated_job_debate_turn_hashes(
                session,
                job_id=lease.job_id,
                stock_code=stock_code,
                evidence_snapshot_hash=evidence_snapshot_hash,
                request_hash=request_hash,
                prompt_version=prompt_version,
                prompt_fingerprints=prompt_fingerprints,
                model_route_fingerprint=model_route_fingerprint,
                as_of=as_of,
            )
            if persisted_turn_hashes != expected_bound_turns:
                raise ValueError(
                    "debate snapshot turns conflict with durable stance checkpoints"
                )
            overlap = set(persisted_turn_hashes).intersection(
                persisted_failures
            )
            if overlap:
                raise ValueError(
                    "Debate stance has both successful and failed checkpoints: "
                    + ",".join(sorted(overlap))
                )

            expected_turn_hashes = [
                digest
                for digest in (bull_turn_hash, bear_turn_hash)
                if digest is not None
            ]
            turn_rows = (
                session.execute(
                    select(ResearchDebateTurnRecord).where(
                        ResearchDebateTurnRecord.turn_hash.in_(
                            expected_turn_hashes
                        )
                    )
                ).scalars().all()
                if expected_turn_hashes
                else []
            )
            turns_by_hash = {row.turn_hash: row for row in turn_rows}
            missing_turns = sorted(
                set(expected_turn_hashes).difference(turns_by_hash)
            )
            if missing_turns:
                raise ValueError(
                    "debate turn hashes do not reference stored turns: "
                    + ",".join(missing_turns)
                )
            expected_projections: list[dict[str, Any]] = []
            expected_counts = {
                "bull_argument_count": 0,
                "bear_argument_count": 0,
                "open_question_count": 0,
            }
            from .debate_service import hydrate_debate_turn

            for expected_stance, digest in (
                ("bull", bull_turn_hash),
                ("bear", bear_turn_hash),
            ):
                if digest is None:
                    continue
                row = turns_by_hash[digest]
                actual_lineage = {
                    "stock_code": row.stock_code,
                    "market": _market_identity(row.market),
                    "stance": row.stance,
                    "debate_engine_version": row.debate_engine_version,
                    "output_schema_version": row.output_schema_version,
                    "prompt_version": row.prompt_version,
                    "evidence_snapshot_hash": row.evidence_snapshot_hash,
                    "request_hash": row.request_hash,
                    "model_route_fingerprint": row.model_route_fingerprint,
                }
                expected_lineage = {
                    "stock_code": stock_code,
                    "market": _market_identity(market),
                    "stance": expected_stance,
                    "debate_engine_version": debate_engine_version,
                    "output_schema_version": output_schema_version,
                    "prompt_version": prompt_version,
                    "evidence_snapshot_hash": evidence_snapshot_hash,
                    "request_hash": request_hash,
                    "model_route_fingerprint": model_route_fingerprint,
                }
                if actual_lineage != expected_lineage:
                    raise ValueError(
                        f"{expected_stance}_turn_hash conflicts with debate lineage"
                    )
                if row.as_of > as_of or row.available_at > available_at:
                    raise ValueError(
                        f"{expected_stance}_turn_hash is after debate boundary"
                    )
                hydrated_turn = hydrate_debate_turn(
                    _debate_turn_record_dict(row),
                    request=hydrated_request,
                )
                if hydrated_turn.turn_hash != digest:
                    raise ValueError("stored debate turn payload conflicts with turn_hash")
                projection = _debate_turn_projection(row)
                expected_projections.append(projection)
                arguments = projection["arguments"]
                questions = projection["open_questions"]
                expected_counts[f"{expected_stance}_argument_count"] = len(arguments)
                expected_counts["open_question_count"] += len(questions)

            if expected_counts != {
                "bull_argument_count": bull_argument_count,
                "bear_argument_count": bear_argument_count,
                "open_question_count": open_question_count,
            }:
                raise ValueError("debate snapshot counts conflict with stored turns")
            if canonical_json(
                debate_payload.get("turns"),
                exclude_volatile=False,
            ) != canonical_json(expected_projections, exclude_volatile=False):
                raise ValueError(
                    "debate snapshot turn projection conflicts with stored turns"
                )

            existing = session.execute(
                select(ResearchDebateSnapshotRecord).where(
                    ResearchDebateSnapshotRecord.debate_hash == debate_hash
                )
            ).scalar_one_or_none()
            event_payload = {
                "stock_code": stock_code,
                "as_of": _utc_text(as_of),
                "evidence_snapshot_hash": evidence_snapshot_hash,
                "request_hash": request_hash,
                "prompt_version": prompt_version,
                "model_route_fingerprint": model_route_fingerprint,
                "debate_hash": debate_hash,
                "status": status,
            }
            if existing is not None:
                hydrate_debate_snapshot(
                    _debate_record_dict(existing),
                    request=hydrated_request,
                )
                self._bind_artifact_to_job(
                    session,
                    lease=lease,
                    event_type="research_debate_snapshot",
                    stage="research_debate",
                    payload=event_payload,
                    now=current,
                )
                return SnapshotWriteResult(int(existing.id), debate_hash, False)
            row = ResearchDebateSnapshotRecord(
                **values,
                debate_hash=debate_hash,
                origin_job_id=lease.job_id,
                created_at=current,
            )
            session.add(row)
            session.flush()
            self._bind_artifact_to_job(
                session,
                lease=lease,
                event_type="research_debate_snapshot",
                stage="research_debate",
                payload=event_payload,
                now=current,
            )
            return SnapshotWriteResult(int(row.id), debate_hash, True)

        return self.db._run_write_transaction(
            "write research debate snapshot",
            _write,
        )

    def write_research_snapshot(
        self,
        snapshot: ResearchSnapshotInput,
        *,
        lease: LeaseFence,
        now: Optional[datetime] = None,
    ) -> SnapshotWriteResult:
        status = normalize_status(snapshot.status)
        as_of = _utc_datetime(snapshot.as_of, "as_of")
        available_at = _utc_datetime(snapshot.available_at, "available_at")
        if available_at > as_of:
            raise ValueError("available_at cannot be after as_of")
        canonical_payload = canonicalize(snapshot.canonical_payload)
        if not isinstance(canonical_payload, Mapping):
            raise ValueError("canonical_payload must be an object")
        dataset_hashes = _research_dataset_hashes(
            canonical_payload.get("datasets")
        )
        factor_snapshot_hash = _optional_sha256(
            snapshot.factor_snapshot_hash, "factor_snapshot_hash"
        )
        evidence_snapshot_hash = _optional_sha256(
            snapshot.evidence_snapshot_hash,
            "evidence_snapshot_hash",
        )
        debate_snapshot_hash = _optional_sha256(
            snapshot.debate_snapshot_hash,
            "debate_snapshot_hash",
        )
        has_evidence_projection = (
            isinstance(canonical_payload, Mapping)
            and "evidence" in canonical_payload
        )
        if evidence_snapshot_hash is None and has_evidence_projection:
            raise ValueError(
                "canonical_payload cannot carry evidence without evidence_snapshot_hash"
            )
        if evidence_snapshot_hash is not None and (
            not has_evidence_projection
            or not isinstance(canonical_payload.get("evidence"), Mapping)
        ):
            raise ValueError(
                "evidence_snapshot_hash requires a frozen evidence projection"
            )
        if evidence_snapshot_hash is not None and factor_snapshot_hash is None:
            raise ValueError(
                "evidence_snapshot_hash requires factor_snapshot_hash lineage"
            )
        factor_projection = canonical_payload.get("factors")
        if factor_snapshot_hash is None and factor_projection not in (None, {}):
            raise ValueError(
                "canonical_payload cannot carry factors without factor_snapshot_hash"
            )
        if factor_snapshot_hash is not None and not isinstance(
            factor_projection,
            Mapping,
        ):
            raise ValueError(
                "factor_snapshot_hash requires a frozen factors projection"
            )
        has_debate_projection = (
            isinstance(canonical_payload, Mapping)
            and "debate" in canonical_payload
        )
        if debate_snapshot_hash is None and has_debate_projection:
            raise ValueError(
                "canonical_payload cannot carry debate without debate_snapshot_hash"
            )
        if debate_snapshot_hash is not None and (
            not has_debate_projection
            or not isinstance(canonical_payload.get("debate"), Mapping)
        ):
            raise ValueError(
                "debate_snapshot_hash requires a frozen debate projection"
            )
        if debate_snapshot_hash is not None and evidence_snapshot_hash is None:
            raise ValueError(
                "debate_snapshot_hash requires evidence_snapshot_hash lineage"
            )
        values = {
            "stock_code": _required_text(snapshot.stock_code, "stock_code", max_length=16),
            "market": _required_text(snapshot.market, "market", max_length=16),
            "snapshot_version": _research_version(
                snapshot.snapshot_version, "snapshot_version"
            ),
            "field_dictionary_version": _research_version(
                snapshot.field_dictionary_version,
                "field_dictionary_version",
            ),
            "factor_engine_version": _research_version(
                snapshot.factor_engine_version,
                "factor_engine_version",
            ),
            "pack_version": _research_version(
                snapshot.pack_version, "pack_version"
            ),
            "prompt_version": _research_version(
                snapshot.prompt_version, "prompt_version"
            ),
            "policy_version": _research_version(
                snapshot.policy_version, "policy_version"
            ),
            "model_route_fingerprint": _research_public_identifier(
                snapshot.model_route_fingerprint,
                "model_route_fingerprint",
            ),
            "as_of": as_of,
            "available_at": available_at,
            "status": status,
            "canonical_json": canonical_json(canonical_payload),
            "factor_snapshot_hash": factor_snapshot_hash,
            "evidence_snapshot_hash": evidence_snapshot_hash,
            "debate_snapshot_hash": debate_snapshot_hash,
        }
        identity_values = dict(values)
        if evidence_snapshot_hash is None:
            # Preserve the exact PR2 content identity while evidence is off.
            identity_values.pop("evidence_snapshot_hash")
        if debate_snapshot_hash is None:
            # Preserve the exact PR3 v2 content identity while debate is off.
            identity_values.pop("debate_snapshot_hash")
        snapshot_hash = canonical_hash(
            {
                **identity_values,
                "canonical_json": canonical_payload,
            }
        )
        binding_payload = {
            "stock_code": values["stock_code"],
            "as_of": as_of.isoformat(timespec="microseconds") + "Z",
            "snapshot_hash": snapshot_hash,
            "status": status,
        }
        if evidence_snapshot_hash is not None:
            binding_payload["evidence_snapshot_hash"] = evidence_snapshot_hash
        if debate_snapshot_hash is not None:
            binding_payload["debate_snapshot_hash"] = debate_snapshot_hash
        requested_now = _utc_datetime(now, "now") if now is not None else None

        def _write(session) -> SnapshotWriteResult:
            current = requested_now or utc_naive_now()
            self._assert_live_lease(
                session,
                lease,
                current,
                stock_code=values["stock_code"],
            )
            dataset_rows = self._validate_stored_dataset_sources(
                session,
                stock_code=values["stock_code"],
                market=values["market"],
                as_of=as_of,
                dataset_hashes=dataset_hashes,
            )
            self._validate_research_dataset_projection(
                projection=canonical_payload["datasets"],
                datasets=dataset_rows,
                as_of=as_of,
            )
            self._require_job_dataset_bindings(
                session,
                job_id=lease.job_id,
                datasets=dataset_rows,
                consumer_as_of=as_of,
                require_exact_hash=True,
            )
            factor_row = None
            if factor_snapshot_hash is not None:
                factor_row, factor_datasets = self._validate_stored_factor_source(
                    session,
                    stock_code=values["stock_code"],
                    market=values["market"],
                    as_of=as_of,
                    factor_snapshot_hash=factor_snapshot_hash,
                )
                if {
                    row.content_hash for row in factor_datasets
                } != set(dataset_hashes) and evidence_snapshot_hash is None:
                    raise ValueError(
                        "Research snapshot datasets differ from Factor lineage"
                    )
                self._validate_research_factor_projection(
                    factor=factor_row,
                    factor_engine_version=values["factor_engine_version"],
                    factor_projection=factor_projection,
                    as_of=as_of,
                )
                self._require_job_factor_binding(
                    session,
                    job_id=lease.job_id,
                    factor=factor_row,
                )
            if evidence_snapshot_hash is not None:
                evidence_row = self._validate_research_evidence_reference(
                    session,
                    stock_code=values["stock_code"],
                    market=values["market"],
                    as_of=as_of,
                    evidence_snapshot_hash=evidence_snapshot_hash,
                    evidence_projection=canonical_payload["evidence"],
                )
                if (
                    factor_row is None
                    or evidence_row.factor_snapshot_hash != factor_row.content_hash
                ):
                    raise ValueError(
                        "Evidence and Research snapshot reference different Factors"
                    )
                evidence_dataset_hashes = set(
                    _json_value(evidence_row.input_dataset_hashes_json)
                )
                if evidence_dataset_hashes != set(dataset_hashes):
                    raise ValueError(
                        "Research snapshot datasets differ from Evidence lineage"
                    )
                self._require_job_evidence_binding(
                    session,
                    job_id=lease.job_id,
                    evidence=evidence_row,
                )
            if debate_snapshot_hash is not None:
                debate_row = self._validate_research_debate_reference(
                    session,
                    stock_code=values["stock_code"],
                    market=values["market"],
                    as_of=as_of,
                    evidence_snapshot_hash=evidence_snapshot_hash,
                    debate_snapshot_hash=debate_snapshot_hash,
                    debate_projection=canonical_payload["debate"],
                )
                self._require_job_debate_snapshot_binding(
                    session,
                    job_id=lease.job_id,
                    debate=debate_row,
                )
            existing = session.execute(
                select(ResearchSnapshotRecord).where(
                    ResearchSnapshotRecord.snapshot_hash == snapshot_hash
                )
            ).scalar_one_or_none()
            if existing is not None:
                self._validate_stored_research_snapshot(session, existing)
                self._bind_artifact_to_job(
                    session,
                    lease=lease,
                    event_type="research_snapshot",
                    stage="research_snapshot",
                    payload=binding_payload,
                    now=current,
                )
                return SnapshotWriteResult(int(existing.id), snapshot_hash, False)
            row = ResearchSnapshotRecord(
                **values,
                snapshot_hash=snapshot_hash,
                origin_job_id=lease.job_id,
                created_at=current,
            )
            session.add(row)
            session.flush()
            self._bind_artifact_to_job(
                session,
                lease=lease,
                event_type="research_snapshot",
                stage="research_snapshot",
                payload=binding_payload,
                now=current,
            )
            return SnapshotWriteResult(int(row.id), snapshot_hash, True)

        return self.db._run_write_transaction("write immutable research snapshot", _write)

    @staticmethod
    def _validate_research_factor_projection(
        *,
        factor: ResearchFactorSnapshotRecord,
        factor_engine_version: str,
        factor_projection: Mapping[str, Any],
        as_of: datetime,
    ) -> None:
        """Require the final snapshot to freeze the exact pinned Factor payload."""

        stored_version = _research_version(
            factor.engine_bundle_version,
            "engine_bundle_version",
        )
        if factor_engine_version != stored_version:
            raise ValueError(
                "Research snapshot factor_engine_version conflicts with Factors"
            )
        from .snapshot_service import project_factors

        expected_projection = project_factors(
            _json_value(factor.factor_json),
            as_of=as_of.replace(tzinfo=timezone.utc),
        )
        if canonical_json(
            factor_projection,
            exclude_volatile=False,
        ) != canonical_json(expected_projection, exclude_volatile=False):
            raise ValueError(
                "frozen factors projection conflicts with factor_snapshot_hash"
            )

    @staticmethod
    def _validate_research_dataset_projection(
        *,
        projection: Mapping[str, Any],
        datasets: Sequence[ResearchDatasetSnapshotRecord],
        as_of: datetime,
    ) -> None:
        """Rebuild the complete prompt-visible Dataset projection from rows."""

        from .snapshot_service import project_structured_datasets

        by_hash = {row.content_hash: row for row in datasets}
        projected_hashes: set[str] = set()
        for dataset_name, raw_item in projection.items():
            normalized_name = _required_text(
                dataset_name,
                "dataset projection name",
                max_length=64,
            )
            if not isinstance(raw_item, Mapping):
                raise ValueError("each frozen dataset projection must be an object")
            current_hash = _sha256(
                raw_item.get("content_hash"),
                "dataset content_hash",
            )
            raw_hashes = raw_item.get("content_hashes")
            if isinstance(raw_hashes, (str, bytes, bytearray)) or not isinstance(
                raw_hashes,
                Sequence,
            ):
                raise ValueError("dataset content_hashes must be an array")
            normalized_hashes = tuple(
                _sha256(value, "dataset content_hash") for value in raw_hashes
            )
            item_hashes = set(normalized_hashes)
            if normalized_hashes != tuple(sorted(item_hashes)):
                raise ValueError(
                    "dataset content_hashes must be sorted and unique"
                )
            if current_hash not in item_hashes:
                raise ValueError(
                    "dataset content_hash must be present in content_hashes"
                )
            projected_hashes.update(item_hashes)
            item_rows = [by_hash.get(content_hash) for content_hash in sorted(item_hashes)]
            if any(row is None for row in item_rows):
                raise ValueError("frozen dataset projection references missing content")
            typed_rows = [row for row in item_rows if row is not None]
            if any(row.dataset != normalized_name for row in typed_rows):
                raise ValueError("frozen dataset projection mixes dataset identities")
            if raw_item.get("dataset") not in {None, normalized_name}:
                raise ValueError("frozen dataset projection name is inconsistent")

            current_row = by_hash[current_hash]
            unique_rows: dict[str, Mapping[str, Any]] = {}
            for row in typed_rows:
                payload = _json_value(row.normalized_json)
                candidates = (
                    payload
                    if isinstance(payload, list)
                    else [payload]
                    if isinstance(payload, Mapping)
                    else []
                )
                for candidate in candidates:
                    if not isinstance(candidate, Mapping):
                        raise ValueError(
                            "stored dataset normalized rows must be objects"
                        )
                    unique_rows.setdefault(
                        canonical_json(candidate, exclude_volatile=False),
                        candidate,
                    )
            expected_rows = [
                canonicalize(candidate, exclude_volatile=False)
                for _, candidate in sorted(
                    unique_rows.items(),
                    key=lambda item: (
                        str(item[1].get("trade_date") or ""),
                        item[0],
                    ),
                )
            ]
            if len(typed_rows) == 1:
                expected_status = current_row.status
            else:
                statuses = {row.status for row in typed_rows}
                latest_row = max(
                    typed_rows,
                    key=lambda row: (
                        row.trade_date or date.min,
                        row.available_at,
                        int(row.id),
                    ),
                )
                expected_status = (
                    "partial"
                    if "partial" in statuses
                    else "stale"
                    if latest_row.status == "stale"
                    else "available"
                )
            expected_raw = {
                normalized_name: {
                    "dataset": normalized_name,
                    "status": expected_status,
                    "row_count": len(expected_rows),
                    "available_at": max(
                        row.available_at for row in typed_rows
                    ).replace(tzinfo=timezone.utc),
                    "data_as_of": max(
                        row.data_as_of for row in typed_rows
                    ).replace(tzinfo=timezone.utc),
                    "content_hash": current_hash,
                    "content_hashes": tuple(sorted(item_hashes)),
                    "raw_ref": _json_value(current_row.raw_ref_json),
                    "rows": expected_rows,
                }
            }
            expected_projection = project_structured_datasets(
                expected_raw,
                as_of=as_of.replace(tzinfo=timezone.utc),
            )[normalized_name]
            if canonical_json(
                raw_item,
                exclude_volatile=False,
            ) != canonical_json(expected_projection, exclude_volatile=False):
                raise ValueError(
                    "frozen dataset projection conflicts with stored snapshots"
                )
        if projected_hashes != set(by_hash):
            raise ValueError("frozen dataset projection lineage is incomplete")

    @staticmethod
    def _validate_research_evidence_reference(
        session,
        *,
        stock_code: str,
        market: str,
        as_of: datetime,
        evidence_snapshot_hash: str,
        evidence_projection: Mapping[str, Any],
    ) -> ResearchEvidenceSnapshotRecord:
        evidence, stored = ResearchSnapshotRepository._validate_stored_evidence_for_debate(
            session,
            stock_code=stock_code,
            market=market,
            as_of=as_of,
            evidence_snapshot_hash=evidence_snapshot_hash,
        )
        from .snapshot_service import project_evidence

        stored_payload = stored.canonical_payload
        expected_projection = project_evidence(
            stored_payload,
            as_of=as_of.replace(tzinfo=timezone.utc),
        )
        if canonical_json(
            evidence_projection,
            exclude_volatile=False,
        ) != canonical_json(expected_projection, exclude_volatile=False):
            raise ValueError(
                "frozen evidence projection conflicts with evidence_snapshot_hash"
            )
        return evidence

    @classmethod
    def _validate_research_debate_reference(
        cls,
        session,
        *,
        stock_code: str,
        market: str,
        as_of: datetime,
        evidence_snapshot_hash: str,
        debate_snapshot_hash: str,
        debate_projection: Mapping[str, Any],
    ) -> ResearchDebateSnapshotRecord:
        debate = session.execute(
            select(ResearchDebateSnapshotRecord).where(
                ResearchDebateSnapshotRecord.debate_hash
                == debate_snapshot_hash
            )
        ).scalar_one_or_none()
        if debate is None:
            raise ValueError(
                "debate_snapshot_hash does not reference a stored debate"
            )
        if (
            debate.stock_code != stock_code
            or _market_identity(debate.market) != _market_identity(market)
        ):
            raise ValueError(
                "debate_snapshot_hash belongs to a different stock or market"
            )
        if debate.evidence_snapshot_hash != evidence_snapshot_hash:
            raise ValueError(
                "debate_snapshot_hash conflicts with evidence_snapshot_hash"
            )
        if debate.as_of > as_of or debate.available_at > as_of:
            raise ValueError("debate_snapshot_hash is after research snapshot as_of")

        from .debate_service import (
            hydrate_debate_snapshot,
            hydrate_debate_turn,
        )
        from .snapshot_service import project_debate

        _, hydrated_request, _ = cls._validate_stored_debate_request(
            session,
            request_hash=debate.request_hash,
            stock_code=debate.stock_code,
            market=debate.market,
            debate_engine_version=debate.debate_engine_version,
            output_schema_version=debate.output_schema_version,
            prompt_version=debate.prompt_version,
            evidence_snapshot_hash=debate.evidence_snapshot_hash,
            model_route_fingerprint=debate.model_route_fingerprint,
            as_of=debate.as_of,
        )
        stored = hydrate_debate_snapshot(
            _debate_record_dict(debate),
            request=hydrated_request,
        )
        stored_payload = stored.canonical_payload
        expected_projections: list[dict[str, Any]] = []
        expected_counts = {
            "bull_argument_count": 0,
            "bear_argument_count": 0,
            "open_question_count": 0,
        }
        for expected_stance, digest in (
            ("bull", debate.bull_turn_hash),
            ("bear", debate.bear_turn_hash),
        ):
            if digest is None:
                continue
            turn = session.execute(
                select(ResearchDebateTurnRecord).where(
                    ResearchDebateTurnRecord.turn_hash == digest
                )
            ).scalar_one_or_none()
            if turn is None:
                raise ValueError(
                    f"stored debate references a missing {expected_stance} turn"
                )
            expected_lineage = {
                "stock_code": debate.stock_code,
                "market": _market_identity(debate.market),
                "stance": expected_stance,
                "debate_engine_version": debate.debate_engine_version,
                "output_schema_version": debate.output_schema_version,
                "prompt_version": debate.prompt_version,
                "evidence_snapshot_hash": debate.evidence_snapshot_hash,
                "request_hash": debate.request_hash,
                "model_route_fingerprint": debate.model_route_fingerprint,
            }
            actual_lineage = {
                "stock_code": turn.stock_code,
                "market": _market_identity(turn.market),
                "stance": turn.stance,
                "debate_engine_version": turn.debate_engine_version,
                "output_schema_version": turn.output_schema_version,
                "prompt_version": turn.prompt_version,
                "evidence_snapshot_hash": turn.evidence_snapshot_hash,
                "request_hash": turn.request_hash,
                "model_route_fingerprint": turn.model_route_fingerprint,
            }
            if actual_lineage != expected_lineage:
                raise ValueError("stored debate turn lineage is invalid")
            if turn.as_of > debate.as_of or turn.available_at > debate.available_at:
                raise ValueError("stored debate turn is after its debate boundary")
            hydrated_turn = hydrate_debate_turn(
                _debate_turn_record_dict(turn),
                request=hydrated_request,
            )
            if hydrated_turn.turn_hash != digest:
                raise ValueError("stored debate turn payload conflicts with turn_hash")
            projection = _debate_turn_projection(turn)
            expected_projections.append(projection)
            expected_counts[f"{expected_stance}_argument_count"] = len(
                projection["arguments"]
            )
            expected_counts["open_question_count"] += len(
                projection["open_questions"]
            )
        if expected_counts != {
            "bull_argument_count": int(debate.bull_argument_count),
            "bear_argument_count": int(debate.bear_argument_count),
            "open_question_count": int(debate.open_question_count),
        }:
            raise ValueError("stored debate counts conflict with its turns")
        if canonical_json(
            stored_payload.get("turns"),
            exclude_volatile=False,
        ) != canonical_json(expected_projections, exclude_volatile=False):
            raise ValueError("stored debate projection conflicts with its turns")

        expected_projection = project_debate(
            stored_payload,
            as_of=as_of.replace(tzinfo=timezone.utc),
        )
        if canonical_json(
            debate_projection,
            exclude_volatile=False,
        ) != canonical_json(expected_projection, exclude_volatile=False):
            raise ValueError(
                "frozen debate projection conflicts with debate_snapshot_hash"
            )
        return debate

    @classmethod
    def _validate_stored_research_snapshot(
        cls,
        session,
        row: ResearchSnapshotRecord,
    ) -> None:
        """Recompute the final snapshot and revalidate every pinned artifact."""

        if row.available_at > row.as_of:
            raise ValueError("stored research snapshot available_at is after as_of")
        canonical_payload = _json_value(row.canonical_json)
        if not isinstance(canonical_payload, Mapping):
            raise ValueError("stored research snapshot payload must be an object")
        dataset_hashes = _research_dataset_hashes(
            canonical_payload.get("datasets")
        )
        factor_snapshot_hash = _optional_sha256(
            row.factor_snapshot_hash, "factor_snapshot_hash"
        )
        evidence_snapshot_hash = _optional_sha256(
            row.evidence_snapshot_hash, "evidence_snapshot_hash"
        )
        debate_snapshot_hash = _optional_sha256(
            row.debate_snapshot_hash, "debate_snapshot_hash"
        )
        has_evidence = "evidence" in canonical_payload
        has_debate = "debate" in canonical_payload
        if (evidence_snapshot_hash is None) != (not has_evidence):
            raise ValueError("stored research Evidence linkage is inconsistent")
        if (debate_snapshot_hash is None) != (not has_debate):
            raise ValueError("stored research Debate linkage is inconsistent")
        if debate_snapshot_hash is not None and evidence_snapshot_hash is None:
            raise ValueError("stored Debate linkage requires Evidence")
        values = {
            "stock_code": _required_text(
                row.stock_code, "stock_code", max_length=16
            ),
            "market": _required_text(row.market, "market", max_length=16),
            "snapshot_version": _research_version(
                row.snapshot_version, "snapshot_version"
            ),
            "field_dictionary_version": _research_version(
                row.field_dictionary_version, "field_dictionary_version"
            ),
            "factor_engine_version": _research_version(
                row.factor_engine_version, "factor_engine_version"
            ),
            "pack_version": _research_version(row.pack_version, "pack_version"),
            "prompt_version": _research_version(
                row.prompt_version, "prompt_version"
            ),
            "policy_version": _research_version(
                row.policy_version, "policy_version"
            ),
            "model_route_fingerprint": _research_public_identifier(
                row.model_route_fingerprint,
                "model_route_fingerprint",
            ),
            "as_of": row.as_of,
            "available_at": row.available_at,
            "status": normalize_status(row.status),
            "canonical_json": canonical_payload,
            "factor_snapshot_hash": factor_snapshot_hash,
            "evidence_snapshot_hash": evidence_snapshot_hash,
            "debate_snapshot_hash": debate_snapshot_hash,
        }
        identity_values = dict(values)
        if evidence_snapshot_hash is None:
            identity_values.pop("evidence_snapshot_hash")
        if debate_snapshot_hash is None:
            identity_values.pop("debate_snapshot_hash")
        expected_hash = canonical_hash(identity_values)
        if _sha256(row.snapshot_hash, "snapshot_hash") != expected_hash:
            raise ValueError(
                "stored research snapshot payload conflicts with snapshot_hash"
            )

        factor_row = None
        dataset_rows = cls._validate_stored_dataset_sources(
            session,
            stock_code=row.stock_code,
            market=row.market,
            as_of=row.as_of,
            dataset_hashes=dataset_hashes,
        )
        cls._validate_research_dataset_projection(
            projection=canonical_payload["datasets"],
            datasets=dataset_rows,
            as_of=row.as_of,
        )
        if factor_snapshot_hash is not None:
            factor_projection = canonical_payload.get("factors")
            if not isinstance(factor_projection, Mapping):
                raise ValueError("stored research Factor projection is invalid")
            factor_row, factor_datasets = cls._validate_stored_factor_source(
                session,
                stock_code=row.stock_code,
                market=row.market,
                as_of=row.as_of,
                factor_snapshot_hash=factor_snapshot_hash,
            )
            if {
                dataset.content_hash for dataset in factor_datasets
            } != set(dataset_hashes) and evidence_snapshot_hash is None:
                raise ValueError("stored Research datasets differ from Factor lineage")
            cls._validate_research_factor_projection(
                factor=factor_row,
                factor_engine_version=values["factor_engine_version"],
                factor_projection=factor_projection,
                as_of=row.as_of,
            )
        elif canonical_payload.get("factors") not in (None, {}):
            raise ValueError("stored Research factors have no Factor linkage")
        evidence_row = None
        if evidence_snapshot_hash is not None:
            evidence_projection = canonical_payload.get("evidence")
            if not isinstance(evidence_projection, Mapping):
                raise ValueError("stored research Evidence projection is invalid")
            evidence_row = cls._validate_research_evidence_reference(
                session,
                stock_code=row.stock_code,
                market=row.market,
                as_of=row.as_of,
                evidence_snapshot_hash=evidence_snapshot_hash,
                evidence_projection=evidence_projection,
            )
            if (
                factor_row is None
                or evidence_row.factor_snapshot_hash != factor_row.content_hash
            ):
                raise ValueError("stored research Factor/Evidence lineage conflicts")
            if set(_json_value(evidence_row.input_dataset_hashes_json)) != set(
                dataset_hashes
            ):
                raise ValueError(
                    "stored Research datasets differ from Evidence lineage"
                )
        if debate_snapshot_hash is not None:
            debate_projection = canonical_payload.get("debate")
            if not isinstance(debate_projection, Mapping):
                raise ValueError("stored research Debate projection is invalid")
            cls._validate_research_debate_reference(
                session,
                stock_code=row.stock_code,
                market=row.market,
                as_of=row.as_of,
                evidence_snapshot_hash=evidence_snapshot_hash,
                debate_snapshot_hash=debate_snapshot_hash,
                debate_projection=debate_projection,
            )

    @staticmethod
    def _bind_artifact_to_job(
        session,
        *,
        lease: LeaseFence,
        event_type: str,
        stage: str,
        payload: Mapping[str, Any],
        now: datetime,
    ) -> None:
        """Bind a content-addressed artifact to each consuming job once."""

        payload_json = canonical_json(payload)
        existing = session.execute(
            select(JobEventRecord.id).where(
                JobEventRecord.job_id == lease.job_id,
                JobEventRecord.event_type == event_type,
                JobEventRecord.payload_json == payload_json,
            )
        ).scalar_one_or_none()
        if existing is not None:
            return
        session.add(
            JobEventRecord(
                job_id=lease.job_id,
                event_type=event_type,
                stage=stage,
                payload_json=payload_json,
                created_at=now,
            )
        )

    def list_datasets(
        self,
        *,
        scope_value: str,
        dataset: Optional[str] = None,
        as_of: Optional[datetime] = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Read immutable datasets newest-first without mutating feature state."""

        scope = _required_text(scope_value, "scope_value", max_length=128)
        bounded_limit = max(1, min(int(limit), 500))
        with self.db.get_session() as session:
            statement = select(ResearchDatasetSnapshotRecord).where(
                ResearchDatasetSnapshotRecord.scope_value == scope
            )
            if dataset is not None:
                statement = statement.where(
                    ResearchDatasetSnapshotRecord.dataset
                    == _required_text(dataset, "dataset", max_length=64)
                )
            if as_of is not None:
                cutoff = _utc_datetime(as_of, "as_of")
                statement = statement.where(
                    ResearchDatasetSnapshotRecord.available_at <= cutoff,
                    ResearchDatasetSnapshotRecord.data_as_of <= cutoff,
                    or_(
                        ResearchDatasetSnapshotRecord.dataset != "stock_basic",
                        ResearchDatasetSnapshotRecord.observed_at <= cutoff,
                    ),
                )
            rows = session.execute(
                statement.order_by(
                    ResearchDatasetSnapshotRecord.available_at.desc(),
                    ResearchDatasetSnapshotRecord.id.desc(),
                ).limit(bounded_limit)
            ).scalars().all()
            for row in rows:
                self._validate_stored_dataset_integrity(row)
            return [_dataset_record_dict(row) for row in rows]

    def list_dataset_checkpoints(
        self,
        *,
        scope_value: str,
        dataset: str,
        as_of: datetime,
        trade_date_from: Optional[date] = None,
        trade_date_to: Optional[date] = None,
        statuses: Optional[Sequence[str]] = None,
    ) -> list[dict[str, Any]]:
        """Return every validated immutable Dataset row in one bounded window."""

        scope = _required_text(scope_value, "scope_value", max_length=128)
        dataset_name = _required_text(dataset, "dataset", max_length=64)
        cutoff = _utc_datetime(as_of, "as_of")
        start = _optional_date(trade_date_from, "trade_date_from")
        end = _optional_date(trade_date_to, "trade_date_to")
        if start is not None and end is not None and start > end:
            raise ValueError("trade_date_from cannot be after trade_date_to")
        normalized_statuses = (
            tuple(sorted({normalize_status(value) for value in statuses}))
            if statuses is not None
            else ()
        )
        with self.db.get_session() as session:
            statement = select(ResearchDatasetSnapshotRecord).where(
                ResearchDatasetSnapshotRecord.scope_value == scope,
                ResearchDatasetSnapshotRecord.dataset == dataset_name,
                ResearchDatasetSnapshotRecord.available_at <= cutoff,
                ResearchDatasetSnapshotRecord.data_as_of <= cutoff,
                or_(
                    ResearchDatasetSnapshotRecord.dataset != "stock_basic",
                    ResearchDatasetSnapshotRecord.observed_at <= cutoff,
                ),
            )
            rows = session.execute(
                statement.order_by(
                    ResearchDatasetSnapshotRecord.trade_date.asc(),
                    ResearchDatasetSnapshotRecord.available_at.asc(),
                    ResearchDatasetSnapshotRecord.id.asc(),
                )
            ).scalars().all()
            for row in rows:
                self._validate_stored_dataset_integrity(row)
            selected = [
                row
                for row in rows
                if (
                    (start is None or (row.trade_date is not None and row.trade_date >= start))
                    and (end is None or (row.trade_date is not None and row.trade_date <= end))
                    and (not normalized_statuses or row.status in normalized_statuses)
                )
            ]
            return [_dataset_record_dict(row) for row in selected]

    def get_job_dataset(
        self,
        *,
        job_id: str,
        dataset: str,
        scope_value: str,
        as_of: Optional[datetime] = None,
        terminal_only: bool = False,
    ) -> Optional[dict[str, Any]]:
        """Recover one unambiguous dataset through its durable job binding."""

        normalized_job_id = _required_text(job_id, "job_id", max_length=64)
        normalized_dataset = _required_text(dataset, "dataset", max_length=64)
        normalized_scope = _required_text(
            scope_value,
            "scope_value",
            max_length=128,
        )
        if not isinstance(terminal_only, bool):
            raise TypeError("terminal_only must be a bool")
        requested_cutoff = (
            _utc_datetime(as_of, "as_of") if as_of is not None else None
        )
        with self.db.get_session() as session:
            job = session.execute(
                select(AnalysisJobRecord).where(
                    AnalysisJobRecord.task_id == normalized_job_id
                )
            ).scalar_one_or_none()
            declared_stocks = (
                self._declared_job_stocks(job) if job is not None else set()
            )
            if len(declared_stocks) == 1 and normalized_scope.upper() not in declared_stocks:
                raise ValueError("requested Dataset scope is outside the durable job")
            event_payloads = session.execute(
                select(JobEventRecord.payload_json)
                .where(
                    JobEventRecord.job_id == normalized_job_id,
                    JobEventRecord.event_type == "research_dataset_snapshot",
                )
                .order_by(JobEventRecord.id.desc())
            ).scalars().all()
            candidates: list[tuple[str, datetime, str, bool]] = []
            rows_by_hash: dict[str, ResearchDatasetSnapshotRecord] = {}
            for payload_json in event_payloads:
                try:
                    payload = json.loads(payload_json)
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise ValueError(
                        "stored research dataset binding is invalid JSON"
                    ) from exc
                if not isinstance(payload, Mapping):
                    raise ValueError(
                        "stored research dataset binding must be an object"
                    )
                boundary = self._parse_reference_time(
                    payload.get("knowledge_as_of")
                )
                content_hash = _sha256(
                    payload.get("content_hash"),
                    "content_hash",
                )
                retryable = payload.get("retryable")
                if not isinstance(retryable, bool):
                    raise ValueError(
                        "stored research dataset binding has an invalid retryable flag"
                    )
                bound_dataset = _required_text(
                    payload.get("dataset"),
                    "dataset binding dataset",
                    max_length=64,
                )
                bound_scope_type = _required_text(
                    payload.get("scope_type"),
                    "dataset binding scope_type",
                    max_length=32,
                )
                bound_scope_value = _required_text(
                    payload.get("scope_value"),
                    "dataset binding scope_value",
                    max_length=128,
                )
                bound_status = normalize_status(payload.get("status"))
                if retryable and bound_status != "fetch_failed":
                    raise ValueError(
                        "retryable Dataset bindings must use fetch_failed status"
                    )
                row = rows_by_hash.get(content_hash)
                if row is None:
                    row = session.execute(
                        select(ResearchDatasetSnapshotRecord).where(
                            ResearchDatasetSnapshotRecord.content_hash == content_hash
                        )
                    ).scalar_one_or_none()
                    if row is None:
                        raise ValueError(
                            "research dataset binding references a missing snapshot"
                        )
                    self._validate_stored_dataset_integrity(row)
                    rows_by_hash[content_hash] = row
                if (
                    row.dataset != bound_dataset
                    or row.scope_type != bound_scope_type
                    or row.scope_value != bound_scope_value
                    or row.status != bound_status
                ):
                    raise ValueError(
                        "research dataset binding conflicts with its snapshot"
                    )
                if row.data_as_of > boundary or row.available_at > boundary:
                    raise ValueError(
                        "research dataset binding is after its frozen boundary"
                    )
                if row.dataset == "stock_basic" and row.observed_at > boundary:
                    raise ValueError(
                        "stock_basic binding was observed after its frozen boundary"
                    )
                if len(declared_stocks) == 1 and bound_scope_value.upper() not in declared_stocks:
                    raise ValueError(
                        "research dataset binding is outside the durable job stock scope"
                    )
                if terminal_only and retryable:
                    continue
                if (
                    bound_dataset != normalized_dataset
                    or bound_scope_type != "stock"
                    or bound_scope_value != normalized_scope
                ):
                    continue
                if requested_cutoff is not None and boundary != requested_cutoff:
                    continue
                candidates.append(
                    (content_hash, boundary, bound_status, retryable)
                )
            if not candidates:
                return None
            if requested_cutoff is None:
                distinct_bindings = {
                    (content_hash, boundary)
                    for content_hash, boundary, _status, _retryable in candidates
                }
                if len(distinct_bindings) > 1:
                    raise ValueError(
                        "research dataset binding is ambiguous for job, dataset, and scope"
                    )

            content_hash, _cutoff, _bound_status, retryable = candidates[0]
            result = _dataset_record_dict(rows_by_hash[content_hash])
            result["binding_retryable"] = retryable
            return result

    def get_latest_factors(
        self,
        *,
        stock_code: str,
        as_of: Optional[datetime] = None,
        horizon_days: int = 10,
    ) -> Optional[dict[str, Any]]:
        code = _required_text(stock_code, "stock_code", max_length=16)
        horizon = int(horizon_days)
        if horizon not in {5, 10, 20}:
            raise ValueError("horizon_days must be one of 5, 10, or 20")
        with self.db.get_session() as session:
            statement = select(ResearchFactorSnapshotRecord).where(
                ResearchFactorSnapshotRecord.stock_code == code,
            )
            if as_of is not None:
                cutoff = _utc_datetime(as_of, "as_of")
                statement = statement.where(
                    ResearchFactorSnapshotRecord.as_of <= cutoff,
                    ResearchFactorSnapshotRecord.available_at
                    <= cutoff,
                )
            row = session.execute(
                statement.order_by(
                    ResearchFactorSnapshotRecord.as_of.desc(),
                    ResearchFactorSnapshotRecord.id.desc(),
                ).limit(1)
            ).scalar_one_or_none()
            if row is None:
                return None
            self._validate_stored_factor_source(
                session,
                stock_code=row.stock_code,
                market=row.market,
                as_of=row.as_of,
                factor_snapshot_hash=row.content_hash,
            )
            result = _factor_record_dict(row)
            result["requested_horizon"] = horizon
            result["requested_trend"] = _requested_trend_metric(
                result.get("factors"),
                horizon,
            )
            return result

    def get_research_snapshot(self, snapshot_hash: str) -> Optional[dict[str, Any]]:
        digest = _sha256(snapshot_hash, "snapshot_hash")
        with self.db.get_session() as session:
            row = session.execute(
                select(ResearchSnapshotRecord).where(
                    ResearchSnapshotRecord.snapshot_hash == digest
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            self._validate_stored_research_snapshot(session, row)
            return _research_record_dict(row)

    def get_job_research_snapshot(
        self,
        *,
        job_id: str,
        stock_code: str,
    ) -> Optional[dict[str, Any]]:
        """Recover the one final Research snapshot bound to a durable stock task.

        ``origin_job_id`` only identifies the first writer of a content-addressed
        row, so it is not consumer authority.  Resume must follow the immutable
        job-event binding written in the same transaction as the snapshot.
        Multiple distinct bindings for one task/stock are a contract violation
        and fail closed instead of picking the newest row.
        """

        normalized_job = _required_text(job_id, "job_id", max_length=64)
        normalized_stock = _required_text(
            stock_code,
            "stock_code",
            max_length=16,
        )
        with self.db.get_session() as session:
            payloads = self._job_artifact_event_payloads(
                session,
                job_id=normalized_job,
                event_type="research_snapshot",
            )
            self._validate_job_artifact_stock_scopes(
                session,
                job_id=normalized_job,
                event_type="research_snapshot",
                payloads=payloads,
            )
            bound_hashes = {
                _sha256(payload.get("snapshot_hash"), "snapshot_hash")
                for payload in payloads
                if payload.get("stock_code") == normalized_stock
            }
            if not bound_hashes:
                return None
            if len(bound_hashes) != 1:
                raise ValueError(
                    "durable job contains ambiguous final Research snapshots "
                    f"for stock {normalized_stock!r}"
                )
            digest = next(iter(bound_hashes))
            row = session.execute(
                select(ResearchSnapshotRecord).where(
                    ResearchSnapshotRecord.snapshot_hash == digest
                )
            ).scalar_one_or_none()
            if row is None:
                raise ValueError(
                    "Research snapshot binding references a missing immutable row"
                )
            self._validate_stored_research_snapshot(session, row)
            if row.stock_code != normalized_stock:
                raise ValueError(
                    "Research snapshot binding stock differs from its immutable row"
                )
            matched = [
                payload
                for payload in payloads
                if payload.get("stock_code") == normalized_stock
                and payload.get("snapshot_hash") == digest
            ]
            expected = {
                "stock_code": row.stock_code,
                "as_of": row.as_of.isoformat(timespec="microseconds") + "Z",
                "snapshot_hash": row.snapshot_hash,
                "status": row.status,
            }
            if row.evidence_snapshot_hash is not None:
                expected["evidence_snapshot_hash"] = row.evidence_snapshot_hash
            if row.debate_snapshot_hash is not None:
                expected["debate_snapshot_hash"] = row.debate_snapshot_hash
            if not matched or any(dict(payload) != expected for payload in matched):
                raise ValueError(
                    "Research snapshot binding conflicts with its immutable row"
                )
            return _research_record_dict(row)

    def get_evidence(self, evidence_hash: str) -> Optional[dict[str, Any]]:
        """Read one immutable evidence snapshot by its content identity."""

        digest = _sha256(evidence_hash, "evidence_hash")
        with self.db.get_session() as session:
            row = session.execute(
                select(ResearchEvidenceSnapshotRecord).where(
                    ResearchEvidenceSnapshotRecord.evidence_hash == digest
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            self._validate_stored_evidence_for_debate(
                session,
                stock_code=row.stock_code,
                market=row.market,
                as_of=row.as_of,
                evidence_snapshot_hash=row.evidence_hash,
            )
            return _evidence_record_dict(row)

    def get_debate_request(
        self,
        request_hash: str,
    ) -> Optional[dict[str, Any]]:
        digest = _sha256(request_hash, "request_hash")
        with self.db.get_session() as session:
            row = session.execute(
                select(ResearchDebateRequestRecord).where(
                    ResearchDebateRequestRecord.request_hash == digest
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            self._validate_stored_debate_request(
                session,
                request_hash=row.request_hash,
                stock_code=row.stock_code,
                market=row.market,
                debate_engine_version=row.debate_engine_version,
                output_schema_version=row.output_schema_version,
                prompt_version=row.prompt_version,
                evidence_snapshot_hash=row.evidence_snapshot_hash,
                model_route_fingerprint=row.model_route_fingerprint,
                as_of=row.as_of,
            )
            return _debate_request_record_dict(row)

    def get_debate_turn(self, turn_hash: str) -> Optional[dict[str, Any]]:
        digest = _sha256(turn_hash, "turn_hash")
        with self.db.get_session() as session:
            row = session.execute(
                select(ResearchDebateTurnRecord).where(
                    ResearchDebateTurnRecord.turn_hash == digest
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            _, request, _ = self._validate_stored_debate_request(
                session,
                request_hash=row.request_hash,
                stock_code=row.stock_code,
                market=row.market,
                debate_engine_version=row.debate_engine_version,
                output_schema_version=row.output_schema_version,
                prompt_version=row.prompt_version,
                evidence_snapshot_hash=row.evidence_snapshot_hash,
                model_route_fingerprint=row.model_route_fingerprint,
                as_of=row.as_of,
            )
            from .debate_service import hydrate_debate_turn

            hydrate_debate_turn(_debate_turn_record_dict(row), request=request)
            return _debate_turn_record_dict(row)

    def get_debate_snapshot(
        self,
        debate_hash: str,
    ) -> Optional[dict[str, Any]]:
        digest = _sha256(debate_hash, "debate_hash")
        with self.db.get_session() as session:
            row = session.execute(
                select(ResearchDebateSnapshotRecord).where(
                    ResearchDebateSnapshotRecord.debate_hash == digest
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            from .snapshot_service import project_debate

            debate_payload = _json_value(row.canonical_json)
            self._validate_research_debate_reference(
                session,
                stock_code=row.stock_code,
                market=row.market,
                as_of=row.as_of,
                evidence_snapshot_hash=row.evidence_snapshot_hash,
                debate_snapshot_hash=row.debate_hash,
                debate_projection=project_debate(
                    debate_payload,
                    as_of=row.as_of.replace(tzinfo=timezone.utc),
                ),
            )
            return _debate_record_dict(row)

    @staticmethod
    def _job_artifact_event_payloads(
        session,
        *,
        job_id: str,
        event_type: str,
    ) -> list[Mapping[str, Any]]:
        payload_rows = session.execute(
            select(JobEventRecord.payload_json).where(
                JobEventRecord.job_id == job_id,
                JobEventRecord.event_type == event_type,
            )
        ).scalars().all()
        payloads: list[Mapping[str, Any]] = []
        for payload_json in payload_rows:
            try:
                payload = json.loads(payload_json)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise ValueError(
                    f"stored {event_type} binding is invalid JSON"
                ) from exc
            if not isinstance(payload, Mapping):
                raise ValueError(
                    f"stored {event_type} binding must be an object"
                )
            payloads.append(payload)
        return payloads

    def _validate_job_artifact_stock_scopes(
        self,
        session,
        *,
        job_id: str,
        event_type: str,
        payloads: Sequence[Mapping[str, Any]],
    ) -> None:
        """Reject artifact events outside the durable job's declared stocks."""

        job = session.execute(
            select(AnalysisJobRecord).where(
                AnalysisJobRecord.task_id == job_id
            )
        ).scalar_one_or_none()
        if job is None:
            raise ValueError(f"stored {event_type} binding has no durable job")
        declared_stocks = self._declared_job_stocks(job)
        for payload in payloads:
            bound_stock = _required_text(
                payload.get("stock_code"),
                f"{event_type} binding stock_code",
                max_length=16,
            ).upper()
            if declared_stocks and bound_stock not in declared_stocks:
                raise ValueError(
                    f"stored {event_type} binding is outside the durable job stock scope"
                )

    def _require_job_evidence_binding(
        self,
        session,
        *,
        job_id: str,
        evidence: ResearchEvidenceSnapshotRecord,
    ) -> None:
        """Require this consumer job to bind the exact immutable Evidence."""

        payloads = self._job_artifact_event_payloads(
            session,
            job_id=job_id,
            event_type="research_evidence_snapshot",
        )
        matched_scope = False
        for payload in payloads:
            if payload.get("stock_code") != evidence.stock_code:
                continue
            matched_scope = True
            bound_values = {
                "evidence_hash": _sha256(
                    payload.get("evidence_hash"),
                    "evidence_hash",
                ),
                "as_of": _event_utc_datetime(
                    payload.get("as_of"),
                    "evidence binding as_of",
                ),
                "status": normalize_status(payload.get("status")),
            }
            expected_values = {
                "evidence_hash": evidence.evidence_hash,
                "as_of": evidence.as_of,
                "status": evidence.status,
            }
            if bound_values != expected_values:
                raise ValueError(
                    "current job binds a conflicting Evidence snapshot contract"
                )
        if not matched_scope:
            raise ValueError("current job does not bind the Evidence snapshot")

        factor, factor_datasets = self._validate_stored_factor_source(
            session,
            stock_code=evidence.stock_code,
            market=evidence.market,
            as_of=evidence.as_of,
            factor_snapshot_hash=evidence.factor_snapshot_hash,
        )
        self._require_job_factor_binding(
            session,
            job_id=job_id,
            factor=factor,
        )
        evidence_hashes = _json_value(evidence.input_dataset_hashes_json)
        if not isinstance(evidence_hashes, list):
            raise ValueError("stored evidence input_dataset_hashes are invalid")
        evidence_datasets = self._validate_stored_dataset_sources(
            session,
            stock_code=evidence.stock_code,
            market=evidence.market,
            as_of=evidence.as_of,
            dataset_hashes=evidence_hashes,
        )
        factor_lineage = {row.content_hash for row in factor_datasets}
        direct_datasets = tuple(
            row
            for row in evidence_datasets
            if row.content_hash not in factor_lineage
        )
        self._require_job_dataset_bindings(
            session,
            job_id=job_id,
            datasets=direct_datasets,
            consumer_as_of=evidence.as_of,
            require_exact_hash=True,
        )

    def _require_job_factor_binding(
        self,
        session,
        *,
        job_id: str,
        factor: ResearchFactorSnapshotRecord,
    ) -> None:
        """Require this consumer job to bind the exact immutable Factor."""

        payloads = self._job_artifact_event_payloads(
            session,
            job_id=job_id,
            event_type="research_factor_snapshot",
        )
        matched_scope = False
        for payload in payloads:
            if payload.get("stock_code") != factor.stock_code:
                continue
            matched_scope = True
            bound_values = {
                "content_hash": _sha256(
                    payload.get("content_hash"),
                    "factor content_hash",
                ),
                "as_of": _event_utc_datetime(
                    payload.get("as_of"),
                    "factor binding as_of",
                ),
                "status": normalize_status(payload.get("status")),
            }
            expected_values = {
                "content_hash": factor.content_hash,
                "as_of": factor.as_of,
                "status": factor.status,
            }
            if bound_values != expected_values:
                raise ValueError(
                    "current job binds a conflicting Factor snapshot contract"
                )
        if not matched_scope:
            raise ValueError("current job does not bind the Factor snapshot")
        factor_lineage = _json_value(factor.input_dataset_hashes_json)
        if not isinstance(factor_lineage, list):
            raise ValueError("stored factor input_dataset_hashes are invalid")
        datasets = self._validate_stored_dataset_sources(
            session,
            stock_code=factor.stock_code,
            market=factor.market,
            as_of=factor.as_of,
            dataset_hashes=factor_lineage,
        )
        self._require_job_dataset_bindings(
            session,
            job_id=job_id,
            datasets=datasets,
            consumer_as_of=factor.as_of,
            require_exact_hash=True,
        )

    def _require_job_dataset_bindings(
        self,
        session,
        *,
        job_id: str,
        datasets: Sequence[ResearchDatasetSnapshotRecord],
        consumer_as_of: datetime,
        require_exact_hash: bool,
    ) -> None:
        """Require current-job dataset provenance without flattening CYQ history."""

        if not datasets:
            return
        payloads = self._job_artifact_event_payloads(
            session,
            job_id=job_id,
            event_type="research_dataset_snapshot",
        )
        by_identity: dict[tuple[str, str, str], list[ResearchDatasetSnapshotRecord]] = {}
        for row in datasets:
            identity = (row.dataset, row.scope_type, row.scope_value)
            by_identity.setdefault(identity, []).append(row)

        for identity, rows in by_identity.items():
            expected_by_hash = {row.content_hash: row for row in rows}
            matched_hashes: set[str] = set()
            terminal_hashes: set[str] = set()
            for payload in payloads:
                if (
                    payload.get("dataset"),
                    payload.get("scope_type"),
                    payload.get("scope_value"),
                ) != identity:
                    continue
                content_hash = _sha256(
                    payload.get("content_hash"),
                    "dataset content_hash",
                )
                boundary = _event_utc_datetime(
                    payload.get("knowledge_as_of"),
                    "dataset binding knowledge_as_of",
                )
                retryable = payload.get("retryable")
                if not isinstance(retryable, bool):
                    raise ValueError(
                        "current job dataset binding has an invalid retryable flag"
                    )
                bound_status = normalize_status(payload.get("status"))
                if retryable and bound_status != "fetch_failed":
                    raise ValueError(
                        "retryable Dataset bindings must use fetch_failed status"
                    )
                if not retryable:
                    terminal_hashes.add(content_hash)
                row = expected_by_hash.get(content_hash)
                if row is None:
                    continue
                if bound_status != row.status:
                    raise ValueError(
                        "current job dataset binding conflicts with its snapshot"
                    )
                if boundary > consumer_as_of:
                    raise ValueError(
                        "current job dataset binding is after the consumer boundary"
                    )
                if row.data_as_of > boundary or row.available_at > boundary:
                    raise ValueError(
                        "current job dataset binding predates its stored snapshot"
                    )
                if row.dataset == "stock_basic" and row.observed_at > boundary:
                    raise ValueError(
                        "current job stock_basic binding predates its observation"
                    )
                if not retryable:
                    matched_hashes.add(content_hash)
            if require_exact_hash and terminal_hashes.difference(expected_by_hash):
                raise ValueError(
                    "current job binds a conflicting Dataset snapshot contract"
                )
            if require_exact_hash and set(expected_by_hash).difference(
                matched_hashes
            ):
                raise ValueError(
                    "current job does not bind every required Dataset snapshot"
                )
            if not matched_hashes:
                dataset, scope_type, scope_value = identity
                raise ValueError(
                    "current job does not bind required Dataset snapshot "
                    f"{dataset}:{scope_type}:{scope_value}"
                )

    def _require_job_debate_request_binding(
        self,
        session,
        *,
        job_id: str,
        stock_code: str,
        evidence_snapshot_hash: str,
        request_hash: str,
        prompt_version: str,
        model_route_fingerprint: str,
        as_of: Optional[datetime],
        required: bool = True,
    ) -> bool:
        """Require this consumer job to bind the exact frozen Debate request."""

        payloads = self._job_artifact_event_payloads(
            session,
            job_id=job_id,
            event_type="research_debate_request",
        )
        self._validate_job_artifact_stock_scopes(
            session,
            job_id=job_id,
            event_type="research_debate_request",
            payloads=payloads,
        )
        matched_scope = False
        for payload in payloads:
            if (
                payload.get("stock_code") != stock_code
                or payload.get("evidence_snapshot_hash")
                != evidence_snapshot_hash
            ):
                continue
            matched_scope = True
            if (
                payload.get("prompt_version") != prompt_version
                or payload.get("model_route_fingerprint")
                != model_route_fingerprint
            ):
                raise ValueError(
                    "bound debate request conflicts with the current prompt or route"
                )
            bound_hash = _sha256(payload.get("request_hash"), "request_hash")
            bound_as_of = _event_utc_datetime(
                payload.get("as_of"),
                "request binding as_of",
            )
            if bound_hash != request_hash or (
                as_of is not None and bound_as_of != as_of
            ):
                raise ValueError(
                    "current job binds a conflicting Debate request contract"
                )
        if not matched_scope:
            if required:
                raise ValueError("current job does not bind the Debate request")
            return False
        evidence = session.execute(
            select(ResearchEvidenceSnapshotRecord).where(
                ResearchEvidenceSnapshotRecord.evidence_hash
                == evidence_snapshot_hash
            )
        ).scalar_one_or_none()
        if evidence is None:
            raise ValueError(
                "debate request binding references a missing Evidence snapshot"
            )
        self._require_job_evidence_binding(
            session,
            job_id=job_id,
            evidence=evidence,
        )
        return True

    def _require_job_debate_snapshot_binding(
        self,
        session,
        *,
        job_id: str,
        debate: ResearchDebateSnapshotRecord,
    ) -> None:
        """Require this consumer job to bind the exact completed Debate."""

        payloads = self._job_artifact_event_payloads(
            session,
            job_id=job_id,
            event_type="research_debate_snapshot",
        )
        self._validate_job_artifact_stock_scopes(
            session,
            job_id=job_id,
            event_type="research_debate_snapshot",
            payloads=payloads,
        )
        matched_scope = False
        for payload in payloads:
            if (
                payload.get("stock_code") != debate.stock_code
                or payload.get("evidence_snapshot_hash")
                != debate.evidence_snapshot_hash
            ):
                continue
            matched_scope = True
            bound_values = {
                "request_hash": _sha256(
                    payload.get("request_hash"),
                    "request_hash",
                ),
                "prompt_version": payload.get("prompt_version"),
                "model_route_fingerprint": payload.get(
                    "model_route_fingerprint"
                ),
                "debate_hash": _sha256(
                    payload.get("debate_hash"),
                    "debate_hash",
                ),
                "status": _debate_status(payload.get("status")),
                "as_of": _event_utc_datetime(
                    payload.get("as_of"),
                    "debate binding as_of",
                ),
            }
            expected_values = {
                "request_hash": debate.request_hash,
                "prompt_version": debate.prompt_version,
                "model_route_fingerprint": debate.model_route_fingerprint,
                "debate_hash": debate.debate_hash,
                "status": debate.status,
                "as_of": debate.as_of,
            }
            if bound_values != expected_values:
                raise ValueError(
                    "current job binds a conflicting Debate snapshot contract"
                )
        if not matched_scope:
            raise ValueError("current job does not bind the Debate snapshot")

        _, hydrated_request, _ = self._validate_stored_debate_request(
            session,
            request_hash=debate.request_hash,
            stock_code=debate.stock_code,
            market=debate.market,
            debate_engine_version=debate.debate_engine_version,
            output_schema_version=debate.output_schema_version,
            prompt_version=debate.prompt_version,
            evidence_snapshot_hash=debate.evidence_snapshot_hash,
            model_route_fingerprint=debate.model_route_fingerprint,
            as_of=debate.as_of,
        )
        self._require_job_debate_request_binding(
            session,
            job_id=job_id,
            stock_code=debate.stock_code,
            evidence_snapshot_hash=debate.evidence_snapshot_hash,
            request_hash=debate.request_hash,
            prompt_version=debate.prompt_version,
            model_route_fingerprint=debate.model_route_fingerprint,
            as_of=debate.as_of,
        )
        prompt_fingerprints = {
            item.stance: item.prompt_fingerprint
            for item in hydrated_request.turn_requests
        }
        failures = self._validated_job_debate_failures(
            session,
            job_id=job_id,
            stock_code=debate.stock_code,
            evidence_snapshot_hash=debate.evidence_snapshot_hash,
            request_hash=debate.request_hash,
            prompt_version=debate.prompt_version,
            prompt_fingerprints=prompt_fingerprints,
            model_route_fingerprint=debate.model_route_fingerprint,
            as_of=debate.as_of,
            available_at=debate.available_at,
        )
        turn_hashes = self._validated_job_debate_turn_hashes(
            session,
            job_id=job_id,
            stock_code=debate.stock_code,
            evidence_snapshot_hash=debate.evidence_snapshot_hash,
            request_hash=debate.request_hash,
            prompt_version=debate.prompt_version,
            prompt_fingerprints=prompt_fingerprints,
            model_route_fingerprint=debate.model_route_fingerprint,
            as_of=debate.as_of,
        )
        expected_turn_hashes = {
            stance: digest
            for stance, digest in (
                ("bull", debate.bull_turn_hash),
                ("bear", debate.bear_turn_hash),
            )
            if digest is not None
        }
        if turn_hashes != expected_turn_hashes:
            raise ValueError(
                "Debate snapshot binding conflicts with current-job turn checkpoints"
            )
        overlap = set(turn_hashes).intersection(failures)
        if overlap:
            raise ValueError(
                "Debate stance has both successful and failed checkpoints: "
                + ",".join(sorted(overlap))
            )
        debate_payload = _json_value(debate.canonical_json)
        expected_failures = [
            failures[stance]
            for stance in ("bull", "bear")
            if stance in failures
        ]
        if canonical_json(
            debate_payload.get("failed_stances"),
            exclude_volatile=False,
        ) != canonical_json(expected_failures, exclude_volatile=False):
            raise ValueError(
                "Debate snapshot binding conflicts with current-job failure checkpoints"
            )

    def _validated_job_debate_failures(
        self,
        session,
        *,
        job_id: str,
        stock_code: str,
        evidence_snapshot_hash: str,
        request_hash: str,
        prompt_version: str,
        prompt_fingerprints: Mapping[str, str],
        model_route_fingerprint: str,
        as_of: datetime,
        available_at: datetime,
    ) -> dict[str, dict[str, Any]]:
        payloads = self._job_artifact_event_payloads(
            session,
            job_id=job_id,
            event_type="research_debate_failure",
        )
        self._validate_job_artifact_stock_scopes(
            session,
            job_id=job_id,
            event_type="research_debate_failure",
            payloads=payloads,
        )
        expected_fingerprints = {
            stance: _sha256(
                prompt_fingerprints.get(stance),
                f"{stance}_prompt_fingerprint",
            )
            for stance in ("bull", "bear")
        }
        result: dict[str, dict[str, Any]] = {}
        for payload in payloads:
            if (
                payload.get("stock_code") != stock_code
                or payload.get("evidence_snapshot_hash")
                != evidence_snapshot_hash
            ):
                continue
            if (
                payload.get("request_hash") != request_hash
                or payload.get("prompt_version") != prompt_version
                or payload.get("model_route_fingerprint")
                != model_route_fingerprint
            ):
                raise ValueError(
                    "debate failure binding conflicts with its request contract"
                )
            stance = _debate_stance(payload.get("stance"))
            if payload.get("prompt_fingerprint") != expected_fingerprints[stance]:
                raise ValueError(
                    "debate failure binding conflicts with its frozen prompt"
                )
            if _event_utc_datetime(
                payload.get("as_of"),
                "failure binding as_of",
            ) != as_of or _event_utc_datetime(
                payload.get("available_at"),
                "failure binding available_at",
            ) != available_at:
                raise ValueError(
                    "debate failure binding conflicts with its request boundary"
                )
            error_code = _debate_error_code(payload.get("error_code"))
            candidate = {
                "stance": stance,
                "error_code": error_code,
            }
            existing = result.get(stance)
            if existing is not None and existing != candidate:
                raise ValueError("debate failure binding is ambiguous")
            result[stance] = candidate
        return result

    def _validated_job_debate_turn_hashes(
        self,
        session,
        *,
        job_id: str,
        stock_code: str,
        evidence_snapshot_hash: str,
        request_hash: str,
        prompt_version: str,
        prompt_fingerprints: Mapping[str, str],
        model_route_fingerprint: str,
        as_of: datetime,
    ) -> dict[str, str]:
        payloads = self._job_artifact_event_payloads(
            session,
            job_id=job_id,
            event_type="research_debate_turn",
        )
        self._validate_job_artifact_stock_scopes(
            session,
            job_id=job_id,
            event_type="research_debate_turn",
            payloads=payloads,
        )
        expected_fingerprints = {
            stance: _sha256(
                prompt_fingerprints.get(stance),
                f"{stance}_prompt_fingerprint",
            )
            for stance in ("bull", "bear")
        }
        result: dict[str, str] = {}
        for payload in payloads:
            if (
                payload.get("stock_code") != stock_code
                or payload.get("evidence_snapshot_hash")
                != evidence_snapshot_hash
            ):
                continue
            if (
                payload.get("request_hash") != request_hash
                or payload.get("prompt_version") != prompt_version
                or payload.get("model_route_fingerprint")
                != model_route_fingerprint
            ):
                raise ValueError(
                    "debate turn binding conflicts with its request contract"
                )
            stance = _debate_stance(payload.get("stance"))
            if payload.get("prompt_fingerprint") != expected_fingerprints[stance]:
                raise ValueError(
                    "debate turn binding conflicts with its frozen prompt"
                )
            if _event_utc_datetime(
                payload.get("as_of"),
                "turn binding as_of",
            ) != as_of:
                raise ValueError(
                    "debate turn binding conflicts with its request boundary"
                )
            digest = _sha256(payload.get("turn_hash"), "turn_hash")
            existing = result.get(stance)
            if existing is not None and existing != digest:
                raise ValueError("debate turn binding is ambiguous")
            result[stance] = digest
        return result

    def get_job_debate_request(
        self,
        *,
        job_id: str,
        stock_code: str,
        evidence_snapshot_hash: str,
        prompt_version: str,
        model_route_fingerprint: str,
    ) -> Optional[dict[str, Any]]:
        """Resume only one exact request bound to this job and stock."""

        normalized_job_id = _required_text(job_id, "job_id", max_length=64)
        normalized_stock = _required_text(
            stock_code,
            "stock_code",
            max_length=16,
        )
        evidence_hash = _sha256(
            evidence_snapshot_hash,
            "evidence_snapshot_hash",
        )
        normalized_prompt = _research_version(
            prompt_version,
            "prompt_version",
        )
        normalized_route = _required_text(
            model_route_fingerprint,
            "model_route_fingerprint",
            max_length=128,
        )
        with self.db.get_session() as session:
            payloads = self._job_artifact_event_payloads(
                session,
                job_id=normalized_job_id,
                event_type="research_debate_request",
            )
            self._validate_job_artifact_stock_scopes(
                session,
                job_id=normalized_job_id,
                event_type="research_debate_request",
                payloads=payloads,
            )
            candidates: dict[str, set[datetime]] = {}
            for payload in payloads:
                if (
                    payload.get("stock_code") != normalized_stock
                    or payload.get("evidence_snapshot_hash") != evidence_hash
                ):
                    continue
                if (
                    payload.get("prompt_version") != normalized_prompt
                    or payload.get("model_route_fingerprint") != normalized_route
                ):
                    raise ValueError(
                        "bound debate request conflicts with the current prompt or route"
                    )
                digest = _sha256(payload.get("request_hash"), "request_hash")
                candidates.setdefault(digest, set()).add(
                    _event_utc_datetime(payload.get("as_of"), "request binding as_of")
                )
            if not candidates:
                return None
            if len(candidates) > 1:
                raise ValueError("debate request binding is ambiguous")
            request_hash = next(iter(candidates))
            row = session.execute(
                select(ResearchDebateRequestRecord).where(
                    ResearchDebateRequestRecord.request_hash == request_hash
                )
            ).scalar_one_or_none()
            if row is None:
                raise ValueError("debate request binding references a missing row")
            if candidates[request_hash] != {row.as_of}:
                raise ValueError("debate request binding boundary conflicts with its row")
            if (
                row.stock_code != normalized_stock
                or row.evidence_snapshot_hash != evidence_hash
                or row.prompt_version != normalized_prompt
                or row.model_route_fingerprint != normalized_route
            ):
                raise ValueError("debate request binding conflicts with its row")
            self._validate_stored_debate_request(
                session,
                request_hash=request_hash,
                stock_code=row.stock_code,
                market=row.market,
                debate_engine_version=row.debate_engine_version,
                output_schema_version=row.output_schema_version,
                prompt_version=row.prompt_version,
                evidence_snapshot_hash=row.evidence_snapshot_hash,
                model_route_fingerprint=row.model_route_fingerprint,
                as_of=row.as_of,
            )
            evidence = session.execute(
                select(ResearchEvidenceSnapshotRecord).where(
                    ResearchEvidenceSnapshotRecord.evidence_hash
                    == row.evidence_snapshot_hash
                )
            ).scalar_one()
            self._require_job_evidence_binding(
                session,
                job_id=normalized_job_id,
                evidence=evidence,
            )
            return _debate_request_record_dict(row)

    def get_job_debate_turn(
        self,
        *,
        job_id: str,
        stock_code: str,
        evidence_snapshot_hash: str,
        request_hash: str,
        stance: str,
        prompt_version: str,
        prompt_fingerprint: str,
        model_route_fingerprint: str,
    ) -> Optional[dict[str, Any]]:
        """Resume one exact stance turn through its durable JobEvent binding."""

        normalized_job_id = _required_text(job_id, "job_id", max_length=64)
        normalized_stock = _required_text(
            stock_code,
            "stock_code",
            max_length=16,
        )
        evidence_hash = _sha256(
            evidence_snapshot_hash,
            "evidence_snapshot_hash",
        )
        normalized_request = _sha256(request_hash, "request_hash")
        normalized_stance = _debate_stance(stance)
        normalized_prompt = _research_version(
            prompt_version,
            "prompt_version",
        )
        normalized_prompt_fingerprint = _sha256(
            prompt_fingerprint,
            "prompt_fingerprint",
        )
        normalized_route = _required_text(
            model_route_fingerprint,
            "model_route_fingerprint",
            max_length=128,
        )
        with self.db.get_session() as session:
            request_row = session.execute(
                select(ResearchDebateRequestRecord).where(
                    ResearchDebateRequestRecord.request_hash
                    == normalized_request
                )
            ).scalar_one_or_none()
            has_request_binding = self._require_job_debate_request_binding(
                session,
                job_id=normalized_job_id,
                stock_code=normalized_stock,
                evidence_snapshot_hash=evidence_hash,
                request_hash=normalized_request,
                prompt_version=normalized_prompt,
                model_route_fingerprint=normalized_route,
                as_of=request_row.as_of if request_row is not None else None,
                required=False,
            )
            if not has_request_binding:
                return None
            if request_row is None:
                raise ValueError(
                    "debate request binding references a missing row"
                )
            _, hydrated_request, _ = self._validate_stored_debate_request(
                session,
                request_hash=normalized_request,
                stock_code=normalized_stock,
                market=request_row.market,
                debate_engine_version=request_row.debate_engine_version,
                output_schema_version=request_row.output_schema_version,
                prompt_version=normalized_prompt,
                evidence_snapshot_hash=evidence_hash,
                model_route_fingerprint=normalized_route,
                as_of=request_row.as_of,
            )
            expected_fingerprints = {
                item.stance: item.prompt_fingerprint
                for item in hydrated_request.turn_requests
            }
            if (
                expected_fingerprints.get(normalized_stance)
                != normalized_prompt_fingerprint
            ):
                raise ValueError(
                    "debate turn selector conflicts with its frozen request"
                )
            turn_hashes = self._validated_job_debate_turn_hashes(
                session,
                job_id=normalized_job_id,
                stock_code=normalized_stock,
                evidence_snapshot_hash=evidence_hash,
                request_hash=normalized_request,
                prompt_version=normalized_prompt,
                prompt_fingerprints=expected_fingerprints,
                model_route_fingerprint=normalized_route,
                as_of=request_row.as_of,
            )
            turn_hash = turn_hashes.get(normalized_stance)
            if turn_hash is None:
                return None
            row = session.execute(
                select(ResearchDebateTurnRecord).where(
                    ResearchDebateTurnRecord.turn_hash == turn_hash
                )
            ).scalar_one_or_none()
            if row is None:
                raise ValueError("debate turn binding references a missing row")
            if row.as_of != request_row.as_of:
                raise ValueError("debate turn binding boundary conflicts with its row")
            expected = {
                "stock_code": normalized_stock,
                "evidence_snapshot_hash": evidence_hash,
                "request_hash": normalized_request,
                "stance": normalized_stance,
                "prompt_version": normalized_prompt,
                "prompt_fingerprint": normalized_prompt_fingerprint,
                "model_route_fingerprint": normalized_route,
            }
            actual = {field_name: getattr(row, field_name) for field_name in expected}
            if actual != expected:
                raise ValueError("debate turn binding conflicts with its row")
            request_turns = hydrated_request.canonical_payload.get("turn_requests")
            matching_requests = [
                item
                for item in request_turns
                if isinstance(item, Mapping)
                and item.get("stance") == normalized_stance
            ] if isinstance(request_turns, Sequence) and not isinstance(
                request_turns,
                (str, bytes, bytearray),
            ) else []
            if (
                len(matching_requests) != 1
                or matching_requests[0].get("prompt_fingerprint")
                != normalized_prompt_fingerprint
            ):
                raise ValueError("debate turn conflicts with its frozen request")
            from .debate_service import hydrate_debate_turn

            hydrate_debate_turn(
                _debate_turn_record_dict(row),
                request=hydrated_request,
            )
            return _debate_turn_record_dict(row)

    def get_job_debate_turns(
        self,
        *,
        job_id: str,
        stock_code: str,
        evidence_snapshot_hash: str,
        request_hash: str,
        prompt_version: str,
        prompt_fingerprints: Mapping[str, str],
        model_route_fingerprint: str,
    ) -> dict[str, dict[str, Any]]:
        """Return the unambiguous bull/bear pair already bound to one job."""

        normalized_fingerprints = {
            stance: _sha256(
                prompt_fingerprints.get(stance),
                f"{stance}_prompt_fingerprint",
            )
            for stance in ("bull", "bear")
        }
        result: dict[str, dict[str, Any]] = {}
        for stance in ("bull", "bear"):
            row = self.get_job_debate_turn(
                job_id=job_id,
                stock_code=stock_code,
                evidence_snapshot_hash=evidence_snapshot_hash,
                request_hash=request_hash,
                stance=stance,
                prompt_version=prompt_version,
                prompt_fingerprint=normalized_fingerprints[stance],
                model_route_fingerprint=model_route_fingerprint,
            )
            if row is not None:
                result[stance] = row
        return result

    def get_job_debate_failures(
        self,
        *,
        job_id: str,
        stock_code: str,
        evidence_snapshot_hash: str,
        request_hash: str,
        prompt_version: str,
        prompt_fingerprints: Mapping[str, str],
        model_route_fingerprint: str,
    ) -> dict[str, dict[str, Any]]:
        """Resume terminal stance outcomes without repeating model calls."""

        normalized_job_id = _required_text(job_id, "job_id", max_length=64)
        normalized_stock = _required_text(
            stock_code,
            "stock_code",
            max_length=16,
        )
        evidence_hash = _sha256(
            evidence_snapshot_hash,
            "evidence_snapshot_hash",
        )
        normalized_request = _sha256(request_hash, "request_hash")
        normalized_prompt = _research_version(
            prompt_version,
            "prompt_version",
        )
        normalized_route = _required_text(
            model_route_fingerprint,
            "model_route_fingerprint",
            max_length=128,
        )
        normalized_fingerprints = {
            stance: _sha256(
                prompt_fingerprints.get(stance),
                f"{stance}_prompt_fingerprint",
            )
            for stance in ("bull", "bear")
        }
        with self.db.get_session() as session:
            request_row = session.execute(
                select(ResearchDebateRequestRecord).where(
                    ResearchDebateRequestRecord.request_hash
                    == normalized_request
                )
            ).scalar_one_or_none()
            if request_row is None:
                raise ValueError("debate failure binding references a missing request")
            _, hydrated_request, _ = self._validate_stored_debate_request(
                session,
                request_hash=normalized_request,
                stock_code=normalized_stock,
                market=request_row.market,
                debate_engine_version=request_row.debate_engine_version,
                output_schema_version=request_row.output_schema_version,
                prompt_version=normalized_prompt,
                evidence_snapshot_hash=evidence_hash,
                model_route_fingerprint=normalized_route,
                as_of=request_row.as_of,
            )
            self._require_job_debate_request_binding(
                session,
                job_id=normalized_job_id,
                stock_code=normalized_stock,
                evidence_snapshot_hash=evidence_hash,
                request_hash=normalized_request,
                prompt_version=normalized_prompt,
                model_route_fingerprint=normalized_route,
                as_of=request_row.as_of,
            )
            expected_fingerprints = {
                item.stance: item.prompt_fingerprint
                for item in hydrated_request.turn_requests
            }
            if expected_fingerprints != normalized_fingerprints:
                raise ValueError(
                    "debate failure selectors conflict with the frozen request"
                )
            failures = self._validated_job_debate_failures(
                session,
                job_id=normalized_job_id,
                stock_code=normalized_stock,
                evidence_snapshot_hash=evidence_hash,
                request_hash=normalized_request,
                prompt_version=normalized_prompt,
                prompt_fingerprints=normalized_fingerprints,
                model_route_fingerprint=normalized_route,
                as_of=request_row.as_of,
                available_at=request_row.available_at,
            )
            turn_hashes = self._validated_job_debate_turn_hashes(
                session,
                job_id=normalized_job_id,
                stock_code=normalized_stock,
                evidence_snapshot_hash=evidence_hash,
                request_hash=normalized_request,
                prompt_version=normalized_prompt,
                prompt_fingerprints=normalized_fingerprints,
                model_route_fingerprint=normalized_route,
                as_of=request_row.as_of,
            )
            overlap = set(turn_hashes).intersection(failures)
            if overlap:
                raise ValueError(
                    "Debate stance has both successful and failed bindings: "
                    + ",".join(sorted(overlap))
                )
            return failures

    def list_debate_snapshots(
        self,
        *,
        job_id: Optional[str] = None,
        research_snapshot_hash: Optional[str] = None,
        stock_code: Optional[str] = None,
        evidence_snapshot_hash: Optional[str] = None,
        as_of: Optional[datetime] = None,
        cursor: Optional[str] = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        """List debates with stable keyset pagination and durable bindings."""

        bounded_limit = max(1, min(int(limit), 200))
        normalized_job_id = (
            _required_text(job_id, "job_id", max_length=64)
            if job_id is not None
            else None
        )
        normalized_stock = (
            _required_text(stock_code, "stock_code", max_length=16)
            if stock_code is not None
            else None
        )
        normalized_evidence = (
            _sha256(evidence_snapshot_hash, "evidence_snapshot_hash")
            if evidence_snapshot_hash is not None
            else None
        )
        normalized_research = (
            _sha256(research_snapshot_hash, "research_snapshot_hash")
            if research_snapshot_hash is not None
            else None
        )
        cutoff = _utc_datetime(as_of, "as_of") if as_of is not None else None
        decoded_cursor = (
            _decode_evidence_cursor(cursor) if cursor is not None else None
        )
        with self.db.get_session() as session:
            statement = select(ResearchDebateSnapshotRecord)
            if normalized_job_id is not None:
                payloads = self._job_artifact_event_payloads(
                    session,
                    job_id=normalized_job_id,
                    event_type="research_debate_snapshot",
                )
                bound_hashes = {
                    _sha256(payload.get("debate_hash"), "debate_hash")
                    for payload in payloads
                }
                if not bound_hashes:
                    return {"items": [], "next_cursor": None}
                bound_rows = session.execute(
                    select(ResearchDebateSnapshotRecord).where(
                        ResearchDebateSnapshotRecord.debate_hash.in_(
                            bound_hashes
                        )
                    )
                ).scalars().all()
                rows_by_hash = {row.debate_hash: row for row in bound_rows}
                if set(rows_by_hash) != bound_hashes:
                    raise ValueError(
                        "Debate snapshot binding references a missing row"
                    )
                for bound_row in bound_rows:
                    self._require_job_debate_snapshot_binding(
                        session,
                        job_id=normalized_job_id,
                        debate=bound_row,
                    )
                statement = statement.where(
                    ResearchDebateSnapshotRecord.debate_hash.in_(bound_hashes)
                )
            if normalized_research is not None:
                research_row = session.execute(
                    select(ResearchSnapshotRecord).where(
                        ResearchSnapshotRecord.snapshot_hash == normalized_research
                    )
                ).scalar_one_or_none()
                if research_row is None:
                    return {"items": [], "next_cursor": None}
                self._validate_stored_research_snapshot(session, research_row)
                if research_row.debate_snapshot_hash is None:
                    return {"items": [], "next_cursor": None}
                statement = statement.where(
                    ResearchDebateSnapshotRecord.debate_hash
                    == research_row.debate_snapshot_hash
                )
            if normalized_stock is not None:
                statement = statement.where(
                    ResearchDebateSnapshotRecord.stock_code == normalized_stock
                )
            if normalized_evidence is not None:
                statement = statement.where(
                    ResearchDebateSnapshotRecord.evidence_snapshot_hash
                    == normalized_evidence
                )
            if cutoff is not None:
                statement = statement.where(
                    ResearchDebateSnapshotRecord.as_of <= cutoff,
                    ResearchDebateSnapshotRecord.available_at <= cutoff,
                )
            if decoded_cursor is not None:
                cursor_as_of, cursor_id = decoded_cursor
                statement = statement.where(
                    or_(
                        ResearchDebateSnapshotRecord.as_of < cursor_as_of,
                        (
                            ResearchDebateSnapshotRecord.as_of == cursor_as_of
                        )
                        & (ResearchDebateSnapshotRecord.id < cursor_id),
                    )
                )
            rows = session.execute(
                statement.order_by(
                    ResearchDebateSnapshotRecord.as_of.desc(),
                    ResearchDebateSnapshotRecord.id.desc(),
                ).limit(bounded_limit + 1)
            ).scalars().all()
            has_more = len(rows) > bounded_limit
            visible_rows = rows[:bounded_limit]
            from .snapshot_service import project_debate

            for row in visible_rows:
                payload = _json_value(row.canonical_json)
                self._validate_research_debate_reference(
                    session,
                    stock_code=row.stock_code,
                    market=row.market,
                    as_of=row.as_of,
                    evidence_snapshot_hash=row.evidence_snapshot_hash,
                    debate_snapshot_hash=row.debate_hash,
                    debate_projection=project_debate(
                        payload,
                        as_of=row.as_of.replace(tzinfo=timezone.utc),
                    ),
                )
            next_cursor = None
            if has_more and visible_rows:
                last = visible_rows[-1]
                next_cursor = _encode_evidence_cursor(last.as_of, int(last.id))
            return {
                "items": [_debate_record_dict(row) for row in visible_rows],
                "next_cursor": next_cursor,
            }

    def list_evidence(
        self,
        *,
        job_id: Optional[str] = None,
        research_snapshot_hash: Optional[str] = None,
        stock_code: Optional[str] = None,
        as_of: Optional[datetime] = None,
        cursor: Optional[str] = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        """List evidence with stable keyset pagination and durable bindings."""

        bounded_limit = max(1, min(int(limit), 200))
        normalized_job_id = (
            _required_text(job_id, "job_id", max_length=64)
            if job_id is not None
            else None
        )
        normalized_stock_code = (
            _required_text(stock_code, "stock_code", max_length=16)
            if stock_code is not None
            else None
        )
        normalized_snapshot_hash = (
            _sha256(research_snapshot_hash, "research_snapshot_hash")
            if research_snapshot_hash is not None
            else None
        )
        cutoff = _utc_datetime(as_of, "as_of") if as_of is not None else None
        decoded_cursor = (
            _decode_evidence_cursor(cursor) if cursor is not None else None
        )

        with self.db.get_session() as session:
            statement = select(ResearchEvidenceSnapshotRecord)
            if normalized_job_id is not None:
                payloads = self._job_artifact_event_payloads(
                    session,
                    job_id=normalized_job_id,
                    event_type="research_evidence_snapshot",
                )
                bound_hashes = {
                    _sha256(payload.get("evidence_hash"), "evidence_hash")
                    for payload in payloads
                }
                if not bound_hashes:
                    return {"items": [], "next_cursor": None}
                bound_rows = session.execute(
                    select(ResearchEvidenceSnapshotRecord).where(
                        ResearchEvidenceSnapshotRecord.evidence_hash.in_(
                            bound_hashes
                        )
                    )
                ).scalars().all()
                rows_by_hash = {row.evidence_hash: row for row in bound_rows}
                if set(rows_by_hash) != bound_hashes:
                    raise ValueError(
                        "Evidence snapshot binding references a missing row"
                    )
                for bound_row in bound_rows:
                    self._validate_stored_evidence_for_debate(
                        session,
                        stock_code=bound_row.stock_code,
                        market=bound_row.market,
                        as_of=bound_row.as_of,
                        evidence_snapshot_hash=bound_row.evidence_hash,
                    )
                    self._require_job_evidence_binding(
                        session,
                        job_id=normalized_job_id,
                        evidence=bound_row,
                    )
                statement = statement.where(
                    ResearchEvidenceSnapshotRecord.evidence_hash.in_(bound_hashes)
                )
            if normalized_snapshot_hash is not None:
                research_row = session.execute(
                    select(ResearchSnapshotRecord).where(
                        ResearchSnapshotRecord.snapshot_hash
                        == normalized_snapshot_hash
                    )
                ).scalar_one_or_none()
                if research_row is None:
                    return {"items": [], "next_cursor": None}
                self._validate_stored_research_snapshot(session, research_row)
                if research_row.evidence_snapshot_hash is None:
                    return {"items": [], "next_cursor": None}
                statement = statement.where(
                    ResearchEvidenceSnapshotRecord.evidence_hash
                    == research_row.evidence_snapshot_hash
                )
            if normalized_stock_code is not None:
                statement = statement.where(
                    ResearchEvidenceSnapshotRecord.stock_code
                    == normalized_stock_code
                )
            if cutoff is not None:
                statement = statement.where(
                    ResearchEvidenceSnapshotRecord.as_of <= cutoff,
                    ResearchEvidenceSnapshotRecord.available_at <= cutoff,
                )
            if decoded_cursor is not None:
                cursor_as_of, cursor_id = decoded_cursor
                statement = statement.where(
                    or_(
                        ResearchEvidenceSnapshotRecord.as_of < cursor_as_of,
                        (
                            ResearchEvidenceSnapshotRecord.as_of == cursor_as_of
                        )
                        & (ResearchEvidenceSnapshotRecord.id < cursor_id),
                    )
                )
            rows = session.execute(
                statement.order_by(
                    ResearchEvidenceSnapshotRecord.as_of.desc(),
                    ResearchEvidenceSnapshotRecord.id.desc(),
                ).limit(bounded_limit + 1)
            ).scalars().all()
            has_more = len(rows) > bounded_limit
            visible_rows = rows[:bounded_limit]
            for row in visible_rows:
                self._validate_stored_evidence_for_debate(
                    session,
                    stock_code=row.stock_code,
                    market=row.market,
                    as_of=row.as_of,
                    evidence_snapshot_hash=row.evidence_hash,
                )
            next_cursor = None
            if has_more and visible_rows:
                last = visible_rows[-1]
                next_cursor = _encode_evidence_cursor(last.as_of, int(last.id))
            return {
                "items": [_evidence_record_dict(row) for row in visible_rows],
                "next_cursor": next_cursor,
            }


def _json_value(value: Optional[str]) -> Any:
    if value is None:
        return None
    try:
        return json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("stored research JSON is invalid") from exc


def _datetime_value(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.isoformat() + ("Z" if value.tzinfo is None else "")


def _date_value(value: Optional[date]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def _dataset_record_dict(row: ResearchDatasetSnapshotRecord) -> dict[str, Any]:
    return {
        "id": int(row.id),
        "dataset": row.dataset,
        "scope_type": row.scope_type,
        "scope_value": row.scope_value,
        "market": row.market,
        "provider": row.provider,
        "schema_version": _research_version(row.schema_version, "schema_version"),
        "trade_date": _date_value(row.trade_date),
        "report_date": _date_value(row.report_date),
        "announcement_date": _date_value(row.announcement_date),
        "data_as_of": _datetime_value(row.data_as_of),
        "available_at": _datetime_value(row.available_at),
        "observed_at": _datetime_value(row.observed_at),
        "status": row.status,
        "normalized": _json_value(row.normalized_json),
        "content_hash": row.content_hash,
        "raw_ref": _json_value(row.raw_ref_json),
        "error_code": _optional_research_error_code(row.error_code),
        "error_message": sanitize_error_message(row.error_message_sanitized),
        "supersedes_hash": row.supersedes_hash,
        "origin_job_id": _optional_research_public_identifier(
            row.origin_job_id,
            "origin_job_id",
            max_length=64,
        ),
        "created_at": _datetime_value(row.created_at),
    }


def _factor_record_dict(row: ResearchFactorSnapshotRecord) -> dict[str, Any]:
    return {
        "id": int(row.id),
        "stock_code": row.stock_code,
        "market": row.market,
        "company_profile": row.company_profile,
        "primary_horizon": int(row.primary_horizon),
        "engine_bundle_version": _research_version(
            row.engine_bundle_version,
            "engine_bundle_version",
        ),
        "value_score": row.value_score,
        "quality_score": row.quality_score,
        "trend_score": row.trend_score,
        "catalyst_score": row.catalyst_score,
        "risk_penalty": row.risk_penalty,
        "factors": _json_value(row.factor_json),
        "input_dataset_hashes": _json_value(row.input_dataset_hashes_json),
        "status": row.status,
        "coverage": float(row.coverage),
        "unknowns": _json_value(row.unknowns_json),
        "as_of": _datetime_value(row.as_of),
        "available_at": _datetime_value(row.available_at),
        "content_hash": row.content_hash,
        "origin_job_id": _optional_research_public_identifier(
            row.origin_job_id,
            "origin_job_id",
            max_length=64,
        ),
        "created_at": _datetime_value(row.created_at),
    }


def _evidence_record_dict(
    row: ResearchEvidenceSnapshotRecord,
) -> dict[str, Any]:
    return {
        "id": int(row.id),
        "stock_code": row.stock_code,
        "market": row.market,
        "evidence_engine_version": _research_version(
            row.evidence_engine_version,
            "evidence_engine_version",
        ),
        "claim_policy_version": _research_version(
            row.claim_policy_version,
            "claim_policy_version",
        ),
        "as_of": _datetime_value(row.as_of),
        "available_at": _datetime_value(row.available_at),
        "status": row.status,
        "coverage": float(row.coverage),
        "claim_count": int(row.claim_count),
        "citation_count": int(row.citation_count),
        "evidence": _json_value(row.canonical_json),
        "input_dataset_hashes": _json_value(row.input_dataset_hashes_json),
        "factor_snapshot_hash": row.factor_snapshot_hash,
        "evidence_hash": row.evidence_hash,
        "origin_job_id": _optional_research_public_identifier(
            row.origin_job_id,
            "origin_job_id",
            max_length=64,
        ),
        "created_at": _datetime_value(row.created_at),
    }


def _debate_request_record_dict(
    row: ResearchDebateRequestRecord,
) -> dict[str, Any]:
    return {
        "id": int(row.id),
        "stock_code": row.stock_code,
        "market": row.market,
        "debate_engine_version": _research_version(
            row.debate_engine_version,
            "debate_engine_version",
        ),
        "output_schema_version": _research_version(
            row.output_schema_version,
            "output_schema_version",
        ),
        "prompt_version": _research_version(row.prompt_version, "prompt_version"),
        "as_of": _datetime_value(row.as_of),
        "available_at": _datetime_value(row.available_at),
        "evidence_snapshot_hash": row.evidence_snapshot_hash,
        "model_route_fingerprint": _research_public_identifier(
            row.model_route_fingerprint,
            "model_route_fingerprint",
        ),
        "debate_request": _json_value(row.canonical_json),
        "request_hash": row.request_hash,
        "origin_job_id": _optional_research_public_identifier(
            row.origin_job_id,
            "origin_job_id",
            max_length=64,
        ),
        "created_at": _datetime_value(row.created_at),
    }


def _debate_turn_record_dict(
    row: ResearchDebateTurnRecord,
) -> dict[str, Any]:
    return {
        "id": int(row.id),
        "stock_code": row.stock_code,
        "market": row.market,
        "stance": row.stance,
        "round_no": int(row.round_no),
        "debate_engine_version": _research_version(
            row.debate_engine_version,
            "debate_engine_version",
        ),
        "output_schema_version": _research_version(
            row.output_schema_version,
            "output_schema_version",
        ),
        "prompt_version": _research_version(row.prompt_version, "prompt_version"),
        "as_of": _datetime_value(row.as_of),
        "available_at": _datetime_value(row.available_at),
        "evidence_snapshot_hash": row.evidence_snapshot_hash,
        "request_hash": row.request_hash,
        "prompt_fingerprint": row.prompt_fingerprint,
        "model_route_fingerprint": row.model_route_fingerprint,
        "model_used": row.model_used,
        "debate_turn": _json_value(row.canonical_json),
        "turn_hash": row.turn_hash,
        "origin_job_id": _optional_research_public_identifier(
            row.origin_job_id,
            "origin_job_id",
            max_length=64,
        ),
        "created_at": _datetime_value(row.created_at),
    }


def _debate_turn_projection(row: ResearchDebateTurnRecord) -> dict[str, Any]:
    payload = _json_value(row.canonical_json)
    if not isinstance(payload, Mapping):
        raise ValueError("stored debate turn payload must be an object")
    turn = payload.get("turn")
    if not isinstance(turn, Mapping):
        raise ValueError("stored debate turn payload is missing turn")
    arguments = turn.get("arguments")
    open_questions = turn.get("open_questions")
    if not isinstance(arguments, list) or not isinstance(open_questions, list):
        raise ValueError("stored debate turn collections are invalid")
    return canonicalize(
        {
            "turn_hash": row.turn_hash,
            "model_used": row.model_used,
            "stance": row.stance,
            "prompt_fingerprint": row.prompt_fingerprint,
            "summary": turn.get("summary"),
            "arguments": arguments,
            "open_questions": open_questions,
        },
        exclude_volatile=False,
    )


def _debate_record_dict(
    row: ResearchDebateSnapshotRecord,
) -> dict[str, Any]:
    return {
        "id": int(row.id),
        "stock_code": row.stock_code,
        "market": row.market,
        "debate_engine_version": _research_version(
            row.debate_engine_version,
            "debate_engine_version",
        ),
        "output_schema_version": _research_version(
            row.output_schema_version,
            "output_schema_version",
        ),
        "prompt_version": _research_version(row.prompt_version, "prompt_version"),
        "as_of": _datetime_value(row.as_of),
        "available_at": _datetime_value(row.available_at),
        "status": row.status,
        "evidence_snapshot_hash": row.evidence_snapshot_hash,
        "request_hash": row.request_hash,
        "model_route_fingerprint": row.model_route_fingerprint,
        "bull_turn_hash": row.bull_turn_hash,
        "bear_turn_hash": row.bear_turn_hash,
        "bull_argument_count": int(row.bull_argument_count),
        "bear_argument_count": int(row.bear_argument_count),
        "open_question_count": int(row.open_question_count),
        "debate": _json_value(row.canonical_json),
        "debate_hash": row.debate_hash,
        "origin_job_id": _optional_research_public_identifier(
            row.origin_job_id,
            "origin_job_id",
            max_length=64,
        ),
        "created_at": _datetime_value(row.created_at),
    }


def _requested_trend_metric(
    factors: Any,
    horizon_days: int,
) -> Optional[dict[str, Any]]:
    if not isinstance(factors, Mapping):
        return None
    trend = factors.get("trend_timing")
    if not isinstance(trend, Mapping):
        return None
    metrics = trend.get("metrics")
    if not isinstance(metrics, list):
        return None
    expected_name = f"return_{int(horizon_days)}d"
    for metric in metrics:
        if isinstance(metric, Mapping) and metric.get("name") == expected_name:
            return dict(metric)
    return None


def _research_record_dict(row: ResearchSnapshotRecord) -> dict[str, Any]:
    return {
        "id": int(row.id),
        "stock_code": row.stock_code,
        "market": row.market,
        "snapshot_version": _research_version(
            row.snapshot_version, "snapshot_version"
        ),
        "field_dictionary_version": _research_version(
            row.field_dictionary_version, "field_dictionary_version"
        ),
        "factor_engine_version": _research_version(
            row.factor_engine_version, "factor_engine_version"
        ),
        "pack_version": _research_version(row.pack_version, "pack_version"),
        "prompt_version": _research_version(
            row.prompt_version, "prompt_version"
        ),
        "policy_version": _research_version(
            row.policy_version, "policy_version"
        ),
        "model_route_fingerprint": _research_public_identifier(
            row.model_route_fingerprint,
            "model_route_fingerprint",
        ),
        "as_of": _datetime_value(row.as_of),
        "available_at": _datetime_value(row.available_at),
        "status": row.status,
        "snapshot": _json_value(row.canonical_json),
        "snapshot_hash": row.snapshot_hash,
        "factor_snapshot_hash": row.factor_snapshot_hash,
        "evidence_snapshot_hash": row.evidence_snapshot_hash,
        "debate_snapshot_hash": row.debate_snapshot_hash,
        "origin_job_id": _optional_research_public_identifier(
            row.origin_job_id,
            "origin_job_id",
            max_length=64,
        ),
        "created_at": _datetime_value(row.created_at),
    }


__all__ = [
    "DebateFailureInput",
    "DebateRequestInput",
    "DebateSnapshotInput",
    "DebateTurnInput",
    "DatasetSnapshotInput",
    "EvidenceSnapshotInput",
    "FactorSnapshotInput",
    "LeaseFence",
    "ResearchSnapshotInput",
    "ResearchSnapshotRepository",
    "ResearchReferenceTimeConflictError",
    "ResearchReferenceTimeError",
    "SnapshotWriteResult",
]
