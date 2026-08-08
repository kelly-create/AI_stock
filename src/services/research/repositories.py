"""Lease-fenced repositories for immutable research snapshots."""

from __future__ import annotations

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


def _required_text(value: Any, field_name: str, *, max_length: int) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    if len(normalized) > max_length:
        raise ValueError(f"{field_name} exceeds {max_length} characters")
    return normalized


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
        }
        snapshot_hash = canonical_hash(
            {
                **values,
                "canonical_json": canonical_payload,
            }
        )
        requested_now = _utc_datetime(now, "now") if now is not None else None

        def _write(session) -> SnapshotWriteResult:
            current = requested_now or utc_naive_now()
            self._assert_live_lease(session, lease, current)
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
                    payload={
                        "stock_code": values["stock_code"],
                        "as_of": as_of.isoformat(timespec="microseconds") + "Z",
                        "snapshot_hash": snapshot_hash,
                        "status": status,
                    },
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
                payload={
                    "stock_code": values["stock_code"],
                    "as_of": as_of.isoformat(timespec="microseconds") + "Z",
                    "snapshot_hash": snapshot_hash,
                    "status": status,
                },
                now=current,
            )
            return SnapshotWriteResult(int(row.id), snapshot_hash, True)

        return self.db._run_write_transaction("write immutable research snapshot", _write)

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
        "origin_job_id": row.origin_job_id,
        "created_at": _datetime_value(row.created_at),
    }


__all__ = [
    "DatasetSnapshotInput",
    "FactorSnapshotInput",
    "LeaseFence",
    "ResearchSnapshotInput",
    "ResearchSnapshotRepository",
    "ResearchReferenceTimeConflictError",
    "ResearchReferenceTimeError",
    "SnapshotWriteResult",
]
