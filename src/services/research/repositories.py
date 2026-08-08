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
    ResearchEvidenceSnapshotRecord,
    ResearchFactorSnapshotRecord,
    ResearchSnapshotRecord,
    to_utc_naive_datetime,
    utc_naive_now,
)

from .canonical import canonical_hash, canonical_json, canonicalize
from .datasets import normalize_status, validate_dataset_payload


_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_RESEARCH_REFERENCE_EVENT_TYPE = "research_reference_time"


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
    def _assert_live_lease(session, lease: LeaseFence, now: datetime) -> None:
        job_id = _required_text(lease.job_id, "job_id", max_length=64)
        worker_id = _required_text(lease.worker_id, "worker_id", max_length=128)
        lease_token = _required_text(lease.lease_token, "lease_token", max_length=64)
        live_job = session.execute(
            select(AnalysisJobRecord.task_id).where(
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
            from src.services.durable_jobs import StaleLeaseError

            raise StaleLeaseError(
                f"research snapshot write rejected for stale or cancelled job {job_id!r}"
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
            self._assert_live_lease(session, lease, current)
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
            self._assert_live_lease(session, lease, current)
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
        status = validate_dataset_payload(snapshot.status, snapshot.normalized)
        data_as_of = _utc_datetime(snapshot.data_as_of, "data_as_of")
        available_at = _utc_datetime(snapshot.available_at, "available_at")
        observed_at = _utc_datetime(snapshot.observed_at, "observed_at")
        if available_at > observed_at:
            raise ValueError("available_at cannot be after observed_at")
        knowledge_as_of = (
            _utc_datetime(snapshot.knowledge_as_of, "knowledge_as_of")
            if snapshot.knowledge_as_of is not None
            else available_at
        )
        if available_at > knowledge_as_of:
            raise ValueError("available_at cannot be after knowledge_as_of")
        if data_as_of > knowledge_as_of:
            raise ValueError("data_as_of cannot be after knowledge_as_of")
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
            "dataset": _required_text(snapshot.dataset, "dataset", max_length=64),
            "scope_type": _required_text(snapshot.scope_type, "scope_type", max_length=32),
            "scope_value": _required_text(snapshot.scope_value, "scope_value", max_length=128),
            "market": _required_text(snapshot.market, "market", max_length=16),
            "provider": _required_text(snapshot.provider, "provider", max_length=64),
            "schema_version": _required_text(
                snapshot.schema_version, "schema_version", max_length=64
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
            "error_code": _optional_text(snapshot.error_code, max_length=64),
            "error_message_sanitized": sanitize_error_message(
                snapshot.error_message_sanitized
            ),
            "supersedes_hash": _optional_sha256(
                snapshot.supersedes_hash, "supersedes_hash"
            ),
        }
        # ``observed_at`` and provider wording describe this fetch attempt,
        # not the immutable dataset identity. A reclaimed job that receives
        # the same point-in-time content must converge on the first row.
        identity_values = {
            key: value
            for key, value in values.items()
            if key not in {"observed_at", "error_message_sanitized"}
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
            self._assert_live_lease(session, lease, current)
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
        payload_json = canonical_json(
            {
                "dataset": snapshot.dataset,
                "scope_type": snapshot.scope_type,
                "scope_value": snapshot.scope_value,
                "knowledge_as_of": boundary_text,
                "content_hash": content_hash,
                "status": snapshot.status,
                "retryable": bool(snapshot.retryable),
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
        factor_payload = canonicalize(snapshot.factor_payload)
        unknowns = canonicalize(snapshot.unknowns)
        dataset_hashes = sorted(
            {_sha256(value, "input_dataset_hash") for value in snapshot.input_dataset_hashes}
        )
        values = {
            "stock_code": _required_text(snapshot.stock_code, "stock_code", max_length=16),
            "market": _required_text(snapshot.market, "market", max_length=16),
            "company_profile": _required_text(
                snapshot.company_profile, "company_profile", max_length=32
            ),
            "primary_horizon": primary_horizon,
            "engine_bundle_version": _required_text(
                snapshot.engine_bundle_version,
                "engine_bundle_version",
                max_length=64,
            ),
            "value_score": _score(snapshot.value_score, "value_score"),
            "quality_score": _score(snapshot.quality_score, "quality_score"),
            "trend_score": _score(snapshot.trend_score, "trend_score"),
            "catalyst_score": _score(snapshot.catalyst_score, "catalyst_score"),
            "risk_penalty": _score(snapshot.risk_penalty, "risk_penalty"),
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
            self._assert_live_lease(session, lease, current)
            existing = session.execute(
                select(ResearchFactorSnapshotRecord).where(
                    ResearchFactorSnapshotRecord.content_hash == content_hash
                )
            ).scalar_one_or_none()
            if existing is not None:
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
        evidence_engine_version = _required_text(
            snapshot.evidence_engine_version,
            "evidence_engine_version",
            max_length=64,
        )
        claim_policy_version = _required_text(
            snapshot.claim_policy_version,
            "claim_policy_version",
            max_length=64,
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
            self._assert_live_lease(session, lease, current)
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
            if existing is not None:
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

        factor = session.execute(
            select(ResearchFactorSnapshotRecord).where(
                ResearchFactorSnapshotRecord.content_hash == factor_snapshot_hash
            )
        ).scalar_one_or_none()
        if factor is None:
            raise ValueError("factor_snapshot_hash does not reference stored factors")
        if (
            factor.stock_code != stock_code
            or _market_identity(factor.market) != _market_identity(market)
        ):
            raise ValueError("factor_snapshot_hash belongs to a different stock or market")
        if factor.as_of > as_of or factor.available_at > as_of:
            raise ValueError("factor_snapshot_hash is after evidence as_of")

        datasets = (
            session.execute(
                select(ResearchDatasetSnapshotRecord).where(
                    ResearchDatasetSnapshotRecord.content_hash.in_(dataset_hashes)
                )
            ).scalars().all()
            if dataset_hashes
            else []
        )
        datasets_by_hash = {row.content_hash: row for row in datasets}
        missing_hashes = sorted(set(dataset_hashes).difference(datasets_by_hash))
        if missing_hashes:
            raise ValueError(
                "input_dataset_hashes do not reference stored datasets: "
                + ",".join(missing_hashes)
            )
        for content_hash in dataset_hashes:
            dataset = datasets_by_hash[content_hash]
            if (
                dataset.scope_type != "stock"
                or dataset.scope_value != stock_code
                or _market_identity(dataset.market) != _market_identity(market)
            ):
                raise ValueError(
                    "input dataset belongs to a different stock or market: "
                    + content_hash
                )
            if (
                dataset.data_as_of > as_of
                or dataset.available_at > as_of
                or (
                    dataset.dataset == "stock_basic"
                    and dataset.observed_at > as_of
                )
            ):
                raise ValueError("input dataset is after evidence as_of: " + content_hash)
        factor_lineage = _json_value(factor.input_dataset_hashes_json)
        if not isinstance(factor_lineage, list):
            raise ValueError("stored factor input_dataset_hashes are invalid")
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
        factor_snapshot_hash = _optional_sha256(
            snapshot.factor_snapshot_hash, "factor_snapshot_hash"
        )
        evidence_snapshot_hash = _optional_sha256(
            snapshot.evidence_snapshot_hash,
            "evidence_snapshot_hash",
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
        values = {
            "stock_code": _required_text(snapshot.stock_code, "stock_code", max_length=16),
            "market": _required_text(snapshot.market, "market", max_length=16),
            "snapshot_version": _required_text(
                snapshot.snapshot_version, "snapshot_version", max_length=64
            ),
            "field_dictionary_version": _required_text(
                snapshot.field_dictionary_version,
                "field_dictionary_version",
                max_length=64,
            ),
            "factor_engine_version": _required_text(
                snapshot.factor_engine_version,
                "factor_engine_version",
                max_length=64,
            ),
            "pack_version": _required_text(
                snapshot.pack_version, "pack_version", max_length=64
            ),
            "prompt_version": _required_text(
                snapshot.prompt_version, "prompt_version", max_length=64
            ),
            "policy_version": _required_text(
                snapshot.policy_version, "policy_version", max_length=64
            ),
            "model_route_fingerprint": _required_text(
                snapshot.model_route_fingerprint,
                "model_route_fingerprint",
                max_length=128,
            ),
            "as_of": as_of,
            "available_at": available_at,
            "status": status,
            "canonical_json": canonical_json(canonical_payload),
            "factor_snapshot_hash": factor_snapshot_hash,
            "evidence_snapshot_hash": evidence_snapshot_hash,
        }
        identity_values = dict(values)
        if evidence_snapshot_hash is None:
            # Preserve the exact PR2 content identity while evidence is off.
            identity_values.pop("evidence_snapshot_hash")
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
        requested_now = _utc_datetime(now, "now") if now is not None else None

        def _write(session) -> SnapshotWriteResult:
            current = requested_now or utc_naive_now()
            self._assert_live_lease(session, lease, current)
            if evidence_snapshot_hash is not None:
                self._validate_research_evidence_reference(
                    session,
                    stock_code=values["stock_code"],
                    market=values["market"],
                    as_of=as_of,
                    evidence_snapshot_hash=evidence_snapshot_hash,
                    evidence_projection=canonical_payload["evidence"],
                )
            existing = session.execute(
                select(ResearchSnapshotRecord).where(
                    ResearchSnapshotRecord.snapshot_hash == snapshot_hash
                )
            ).scalar_one_or_none()
            if existing is not None:
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
    def _validate_research_evidence_reference(
        session,
        *,
        stock_code: str,
        market: str,
        as_of: datetime,
        evidence_snapshot_hash: str,
        evidence_projection: Mapping[str, Any],
    ) -> None:
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
        if evidence.stock_code != stock_code or evidence.market != market:
            raise ValueError(
                "evidence_snapshot_hash belongs to a different stock or market"
            )
        if evidence.as_of > as_of or evidence.available_at > as_of:
            raise ValueError("evidence_snapshot_hash is after research snapshot as_of")
        from .evidence_service import hydrate_evidence_snapshot
        from .snapshot_service import project_evidence

        stored = hydrate_evidence_snapshot(_evidence_record_dict(evidence))
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
            return [_dataset_record_dict(row) for row in rows]

    def get_job_dataset(
        self,
        *,
        job_id: str,
        dataset: str,
        scope_value: str,
        as_of: Optional[datetime] = None,
    ) -> Optional[dict[str, Any]]:
        """Recover one unambiguous dataset through its durable job binding."""

        normalized_job_id = _required_text(job_id, "job_id", max_length=64)
        normalized_dataset = _required_text(dataset, "dataset", max_length=64)
        normalized_scope = _required_text(
            scope_value,
            "scope_value",
            max_length=128,
        )
        requested_cutoff = (
            _utc_datetime(as_of, "as_of") if as_of is not None else None
        )
        with self.db.get_session() as session:
            event_payloads = session.execute(
                select(JobEventRecord.payload_json)
                .where(
                    JobEventRecord.job_id == normalized_job_id,
                    JobEventRecord.event_type == "research_dataset_snapshot",
                )
                .order_by(JobEventRecord.id.desc())
            ).scalars().all()
            candidates: list[tuple[str, datetime, Any]] = []
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
                if (
                    payload.get("dataset") != normalized_dataset
                    or payload.get("scope_type") != "stock"
                    or payload.get("scope_value") != normalized_scope
                ):
                    continue
                boundary = self._parse_reference_time(
                    payload.get("knowledge_as_of")
                )
                if requested_cutoff is not None and boundary != requested_cutoff:
                    continue
                content_hash = _sha256(
                    payload.get("content_hash"),
                    "content_hash",
                )
                candidates.append((content_hash, boundary, payload.get("status")))
            if not candidates:
                return None
            if requested_cutoff is None:
                distinct_bindings = {
                    (content_hash, boundary)
                    for content_hash, boundary, _status in candidates
                }
                if len(distinct_bindings) > 1:
                    raise ValueError(
                        "research dataset binding is ambiguous for job, dataset, and scope"
                    )

            content_hash, cutoff, bound_status = candidates[0]
            row = session.execute(
                select(ResearchDatasetSnapshotRecord).where(
                    ResearchDatasetSnapshotRecord.content_hash == content_hash
                )
            ).scalar_one_or_none()
            if row is None:
                raise ValueError(
                    "research dataset binding references a missing snapshot"
                )
            if (
                row.dataset != normalized_dataset
                or row.scope_type != "stock"
                or row.scope_value != normalized_scope
                or row.status != bound_status
            ):
                raise ValueError(
                    "research dataset binding conflicts with its snapshot"
                )
            if row.data_as_of > cutoff or row.available_at > cutoff:
                raise ValueError(
                    "research dataset binding is after its frozen boundary"
                )
            if row.dataset == "stock_basic" and row.observed_at > cutoff:
                raise ValueError(
                    "stock_basic binding was observed after its frozen boundary"
                )
            return _dataset_record_dict(row)

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
            return _research_record_dict(row) if row is not None else None

    def get_evidence(self, evidence_hash: str) -> Optional[dict[str, Any]]:
        """Read one immutable evidence snapshot by its content identity."""

        digest = _sha256(evidence_hash, "evidence_hash")
        with self.db.get_session() as session:
            row = session.execute(
                select(ResearchEvidenceSnapshotRecord).where(
                    ResearchEvidenceSnapshotRecord.evidence_hash == digest
                )
            ).scalar_one_or_none()
            return _evidence_record_dict(row) if row is not None else None

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
                payload_rows = session.execute(
                    select(JobEventRecord.payload_json).where(
                        JobEventRecord.job_id == normalized_job_id,
                        JobEventRecord.event_type == "research_evidence_snapshot",
                    )
                ).scalars().all()
                bound_hashes: set[str] = set()
                for payload_json in payload_rows:
                    try:
                        payload = json.loads(payload_json)
                    except (TypeError, ValueError, json.JSONDecodeError) as exc:
                        raise ValueError(
                            "stored research evidence binding is invalid JSON"
                        ) from exc
                    if not isinstance(payload, Mapping):
                        raise ValueError(
                            "stored research evidence binding must be an object"
                        )
                    bound_hashes.add(
                        _sha256(payload.get("evidence_hash"), "evidence_hash")
                    )
                if not bound_hashes:
                    return {"items": [], "next_cursor": None}
                statement = statement.where(
                    ResearchEvidenceSnapshotRecord.evidence_hash.in_(bound_hashes)
                )
            if normalized_snapshot_hash is not None:
                linked_hash = session.execute(
                    select(ResearchSnapshotRecord.evidence_snapshot_hash).where(
                        ResearchSnapshotRecord.snapshot_hash
                        == normalized_snapshot_hash
                    )
                ).scalar_one_or_none()
                if linked_hash is None:
                    return {"items": [], "next_cursor": None}
                statement = statement.where(
                    ResearchEvidenceSnapshotRecord.evidence_hash == linked_hash
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
        "schema_version": row.schema_version,
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
        "error_code": row.error_code,
        "error_message": row.error_message_sanitized,
        "supersedes_hash": row.supersedes_hash,
        "origin_job_id": row.origin_job_id,
        "created_at": _datetime_value(row.created_at),
    }


def _factor_record_dict(row: ResearchFactorSnapshotRecord) -> dict[str, Any]:
    return {
        "id": int(row.id),
        "stock_code": row.stock_code,
        "market": row.market,
        "company_profile": row.company_profile,
        "primary_horizon": int(row.primary_horizon),
        "engine_bundle_version": row.engine_bundle_version,
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
        "origin_job_id": row.origin_job_id,
        "created_at": _datetime_value(row.created_at),
    }


def _evidence_record_dict(
    row: ResearchEvidenceSnapshotRecord,
) -> dict[str, Any]:
    return {
        "id": int(row.id),
        "stock_code": row.stock_code,
        "market": row.market,
        "evidence_engine_version": row.evidence_engine_version,
        "claim_policy_version": row.claim_policy_version,
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
        "origin_job_id": row.origin_job_id,
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
        "snapshot_version": row.snapshot_version,
        "field_dictionary_version": row.field_dictionary_version,
        "factor_engine_version": row.factor_engine_version,
        "pack_version": row.pack_version,
        "prompt_version": row.prompt_version,
        "policy_version": row.policy_version,
        "model_route_fingerprint": row.model_route_fingerprint,
        "as_of": _datetime_value(row.as_of),
        "available_at": _datetime_value(row.available_at),
        "status": row.status,
        "snapshot": _json_value(row.canonical_json),
        "snapshot_hash": row.snapshot_hash,
        "factor_snapshot_hash": row.factor_snapshot_hash,
        "evidence_snapshot_hash": row.evidence_snapshot_hash,
        "origin_job_id": row.origin_job_id,
        "created_at": _datetime_value(row.created_at),
    }


__all__ = [
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
