"""Fail-closed retention for content-addressed research raw artifacts.

Normalized dataset values and frozen research snapshots remain in SQLite.  This
module only removes raw provider payloads whose *every* database reference has
expired under the configured, dataset-specific policy.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import sqlite3
from typing import Any, Iterable, Mapping, Optional, Sequence
import uuid

from .worker_owner import TushareWorkerOwnerError, TushareWorkerOwnerLock


MARKET_RAW_RETENTION_DAYS = 30
SEARCH_NEWS_RAW_RETENTION_DAYS = 90
ORPHAN_RAW_GRACE_DAYS = 1

# These are the finite-retention Tushare datasets currently collected by PR2.
# Reference/master, financial, forecast, dividend, holder and future event
# datasets intentionally fall through to permanent retention.
MARKET_RAW_DATASETS = frozenset(
    {
        "daily",
        "adj_factor",
        "daily_basic",
        "cyq_perf",
        "cyq_chips",
        "stk_limit",
        "suspend_d",
    }
)

# Future search/evidence ingestion must opt in with one of these explicit names.
# Unknown names are permanent by design, so a typo can retain too much but can
# never silently delete raw evidence too early.
SEARCH_NEWS_RAW_DATASETS = frozenset(
    {
        "search",
        "search_results",
        "news",
        "news_search",
        "articles",
        "intelligence_items",
    }
)

PERMANENT_RAW_DATASETS = frozenset(
    {
        "stock_basic",
        "fina_indicator",
        "income",
        "balancesheet",
        "cashflow",
        "dividend",
        "stk_holdernumber",
        "forecast",
        "event",
        "events",
        "announcements",
        "disclosures",
    }
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CONTENT_PATH_RE = re.compile(
    r"^(?P<prefix>[0-9a-f]{2})/(?P<digest>[0-9a-f]{64})\.(?P<extension>[a-z0-9_]+)\.gz$"
)
_STAGING_PREFIX = ".retention-staging-"
_RESEARCH_DATASET_EVENT_TYPE = "research_dataset_snapshot"


@dataclass(frozen=True)
class RawSnapshotRecord:
    """Minimal row projection used by the retention planner."""

    dataset: str
    raw_ref_json: Any
    observed_at: Any
    created_at: Any
    record_id: Optional[int] = None
    last_referenced_at: Any = None


@dataclass(frozen=True)
class RawRetentionPlan:
    """A deterministic read-only deletion plan."""

    as_of: str
    raw_root: str
    records_scanned: int
    references_scanned: int
    expired_references: int
    protected_references: int
    orphan_paths: tuple[str, ...]
    candidate_paths: tuple[str, ...]
    missing_paths: tuple[str, ...]
    unknown_datasets: tuple[str, ...]
    blocking_errors: tuple[str, ...]

    @property
    def blocked(self) -> bool:
        return bool(self.blocking_errors)


@dataclass(frozen=True)
class RawRetentionResult:
    """Result of either a dry run or an explicit apply."""

    mode: str
    plan: RawRetentionPlan
    deleted_paths: tuple[str, ...] = ()
    reclaimed_bytes: int = 0
    apply_errors: tuple[str, ...] = ()
    staging_directory: Optional[str] = None
    cleanup_pending_paths: tuple[str, ...] = ()

    @property
    def blocked(self) -> bool:
        return self.plan.blocked or bool(self.apply_errors)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["blocked"] = self.blocked
        payload["would_delete_paths"] = list(self.plan.candidate_paths)
        return payload


@dataclass(frozen=True)
class _ParsedReference:
    digest: str
    relative_path: str
    expired: bool


def retention_days_for_dataset(dataset: str) -> Optional[int]:
    """Return finite retention days, or ``None`` for conservative permanence."""

    normalized = str(dataset or "").strip().lower()
    if normalized in MARKET_RAW_DATASETS:
        return MARKET_RAW_RETENTION_DAYS
    if normalized in SEARCH_NEWS_RAW_DATASETS:
        return SEARCH_NEWS_RAW_RETENTION_DAYS
    return None


def _utc_datetime(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{field} is not a valid ISO datetime") from exc
    else:
        raise ValueError(f"{field} is missing")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _retention_anchor(record: RawSnapshotRecord) -> datetime:
    """Keep raw data for the finite window after both observation and storage."""

    observed = _utc_datetime(record.observed_at, field="observed_at")
    created = _utc_datetime(record.created_at, field="created_at")
    values = [observed, created]
    if record.last_referenced_at is not None and record.last_referenced_at != "":
        values.append(
            _utc_datetime(record.last_referenced_at, field="last_referenced_at")
        )
    return max(values)


def _record_label(record: RawSnapshotRecord, index: int) -> str:
    if record.record_id is not None:
        return f"record {record.record_id}"
    return f"record index {index}"


def _raw_reference(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        decoded = value
    elif isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError("raw_ref_json is not valid JSON") from exc
    else:
        raise ValueError("raw_ref_json must be a JSON object")
    if not isinstance(decoded, Mapping):
        raise ValueError("raw_ref_json must decode to an object")
    return decoded


def _safe_relative_path(value: Any, digest: str) -> str:
    relative_path = str(value or "").strip()
    if not relative_path or "\\" in relative_path:
        raise ValueError("raw relative_path must be a non-empty POSIX path")
    pure = PurePosixPath(relative_path)
    if pure.is_absolute() or any(part in {"", ".", ".."} for part in pure.parts):
        raise ValueError("raw relative_path is not safely relative")
    match = _CONTENT_PATH_RE.fullmatch(relative_path)
    if match is None:
        raise ValueError("raw relative_path is not a canonical content-addressed path")
    if match.group("digest") != digest or match.group("prefix") != digest[:2]:
        raise ValueError("raw relative_path does not match content_sha256")
    return relative_path


def _coerce_record(value: RawSnapshotRecord | Mapping[str, Any]) -> RawSnapshotRecord:
    if isinstance(value, RawSnapshotRecord):
        return value
    if not isinstance(value, Mapping):
        raise TypeError("retention records must be RawSnapshotRecord or mapping values")
    return RawSnapshotRecord(
        record_id=value.get("record_id", value.get("id")),
        dataset=str(value.get("dataset") or ""),
        raw_ref_json=value.get("raw_ref_json", value.get("raw_ref")),
        observed_at=value.get("observed_at"),
        created_at=value.get("created_at"),
        last_referenced_at=value.get("last_referenced_at"),
    )


def _target_for_path(raw_root: Path, relative_path: str) -> Path:
    """Return the lexical target after containment and symlink checks."""

    pure = PurePosixPath(relative_path)
    target = raw_root.joinpath(*pure.parts)
    resolved = target.resolve(strict=False)
    if resolved == raw_root or raw_root not in resolved.parents:
        raise ValueError("raw artifact target escapes configured root")

    current = raw_root
    for part in pure.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError("raw artifact path contains a symbolic link")
        if not current.exists():
            break
    return target


def _verify_artifact_payload(target: Path, digest: str) -> int:
    """Verify one gzip artifact without loading its whole payload into memory."""

    if not target.is_file():
        raise ValueError("target is not a regular file")
    content_hash = hashlib.sha256()
    try:
        with gzip.open(target, "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                content_hash.update(chunk)
    except (EOFError, OSError) as exc:
        raise ValueError("raw artifact is not readable gzip data") from exc
    if content_hash.hexdigest() != digest:
        raise ValueError("raw artifact content hash does not match its reference")
    try:
        return int(target.stat().st_size)
    except OSError as exc:
        raise ValueError("raw artifact metadata cannot be read") from exc


def _remove_empty_staging_tree(staging: Path) -> None:
    """Remove an empty staging tree without following or deleting files."""

    if not staging.exists():
        return
    directories = sorted(
        (path for path in staging.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    )
    for directory in directories:
        directory.rmdir()
    staging.rmdir()


def build_raw_retention_plan(
    records: Iterable[RawSnapshotRecord | Mapping[str, Any]],
    raw_root: str | Path,
    *,
    as_of: Optional[datetime] = None,
) -> RawRetentionPlan:
    """Build a fail-closed plan without mutating SQLite or the filesystem."""

    cutoff_time = _utc_datetime(as_of or datetime.now(timezone.utc), field="as_of")
    root = Path(raw_root).expanduser().resolve(strict=False)
    if root == Path(root.anchor):
        raise ValueError("raw_root must not be a filesystem root")

    scanned = 0
    references_scanned = 0
    expired_references = 0
    protected_references = 0
    errors: list[str] = []
    unknown_datasets: set[str] = set()
    references: list[_ParsedReference] = []
    force_protected_hashes: set[str] = set()

    for index, raw_record in enumerate(records):
        scanned += 1
        record = _coerce_record(raw_record)
        if record.raw_ref_json is None or record.raw_ref_json == "":
            continue
        label = _record_label(record, index)
        try:
            raw_ref = _raw_reference(record.raw_ref_json)
        except ValueError as exc:
            errors.append(f"{label}: {exc}")
            continue

        digest = str(raw_ref.get("content_sha256") or "").strip().lower()
        if not _SHA256_RE.fullmatch(digest):
            errors.append(f"{label}: raw_ref_json has an invalid content_sha256")
            continue
        references_scanned += 1
        if str(raw_ref.get("compression") or "").strip().lower() != "gzip":
            force_protected_hashes.add(digest)
            errors.append(f"{label}: raw_ref_json compression must be gzip")
            continue
        try:
            relative_path = _safe_relative_path(raw_ref.get("relative_path"), digest)
            _target_for_path(root, relative_path)
        except ValueError as exc:
            # A valid digest in an invalid reference must still protect any
            # other path carrying the same content from premature deletion.
            force_protected_hashes.add(digest)
            errors.append(f"{label}: {exc}")
            continue

        dataset = str(record.dataset or "").strip().lower()
        retention_days = retention_days_for_dataset(dataset)
        if retention_days is None:
            if dataset not in PERMANENT_RAW_DATASETS:
                unknown_datasets.add(dataset or "<empty>")
            references.append(_ParsedReference(digest, relative_path, expired=False))
            protected_references += 1
            continue

        try:
            anchor = _retention_anchor(record)
        except ValueError as exc:
            force_protected_hashes.add(digest)
            errors.append(f"{label}: {exc}")
            protected_references += 1
            continue

        # The exact cutoff remains retained. Only strictly older records expire.
        expired = anchor < cutoff_time - timedelta(days=retention_days)
        references.append(_ParsedReference(digest, relative_path, expired=expired))
        if expired:
            expired_references += 1
        else:
            protected_references += 1

    protected_hashes = force_protected_hashes | {
        reference.digest for reference in references if not reference.expired
    }
    referenced_paths = {reference.relative_path for reference in references}
    orphan_paths: list[str] = []
    if root.exists():
        try:
            prefix_directories = sorted(root.iterdir(), key=lambda path: path.name)
        except OSError as exc:
            errors.append(f"raw root cannot be enumerated: {exc}")
            prefix_directories = []
        for prefix_directory in prefix_directories:
            if prefix_directory.name.startswith(_STAGING_PREFIX):
                continue
            if not re.fullmatch(r"[0-9a-f]{2}", prefix_directory.name):
                continue
            if prefix_directory.is_symlink() or not prefix_directory.is_dir():
                errors.append(
                    f"raw content prefix {prefix_directory.name}: not a regular directory"
                )
                continue
            try:
                targets = sorted(prefix_directory.iterdir(), key=lambda path: path.name)
            except OSError as exc:
                errors.append(
                    f"raw content prefix {prefix_directory.name}: cannot enumerate: {exc}"
                )
                continue
            for target in targets:
                relative_path = target.relative_to(root).as_posix()
                match = _CONTENT_PATH_RE.fullmatch(relative_path)
                if match is None or relative_path in referenced_paths:
                    continue
                try:
                    _target_for_path(root, relative_path)
                    _verify_artifact_payload(target, match.group("digest"))
                    modified_at = datetime.fromtimestamp(
                        target.stat().st_mtime,
                        tz=timezone.utc,
                    )
                except (OSError, ValueError) as exc:
                    errors.append(f"orphan {relative_path}: {exc}")
                    continue
                if modified_at < cutoff_time - timedelta(days=ORPHAN_RAW_GRACE_DAYS):
                    orphan_paths.append(relative_path)

    candidate_paths = sorted(
        {
            reference.relative_path
            for reference in references
            if reference.expired and reference.digest not in protected_hashes
        }
        | set(orphan_paths)
    )
    candidate_path_set = set(candidate_paths)
    missing_paths: list[str] = []
    references_by_path = {
        reference.relative_path: reference
        for reference in references
    }
    for relative_path, reference in sorted(references_by_path.items()):
        try:
            target = _target_for_path(root, relative_path)
        except ValueError as exc:
            errors.append(f"candidate {relative_path}: {exc}")
            continue
        if not target.exists():
            if reference.digest in protected_hashes:
                errors.append(
                    f"protected reference {relative_path}: raw artifact is missing"
                )
            elif relative_path in candidate_path_set:
                missing_paths.append(relative_path)
            continue
        try:
            _verify_artifact_payload(target, reference.digest)
        except ValueError as exc:
            errors.append(f"reference {relative_path}: {exc}")

    return RawRetentionPlan(
        as_of=cutoff_time.isoformat().replace("+00:00", "Z"),
        raw_root=str(root),
        records_scanned=scanned,
        references_scanned=references_scanned,
        expired_references=expired_references,
        protected_references=protected_references,
        orphan_paths=tuple(sorted(orphan_paths)),
        candidate_paths=tuple(candidate_paths),
        missing_paths=tuple(sorted(missing_paths)),
        unknown_datasets=tuple(sorted(unknown_datasets)),
        blocking_errors=tuple(errors),
    )


def execute_raw_retention(
    records: Iterable[RawSnapshotRecord | Mapping[str, Any]],
    raw_root: str | Path,
    *,
    as_of: Optional[datetime] = None,
    apply: bool = False,
) -> RawRetentionResult:
    """Plan retention and delete only when ``apply=True`` and preflight is clean.

    Eligible files are first atomically moved into a unique directory under
    ``raw_root``.  A staging failure restores every move completed by this run.
    Once all moves complete, canonical paths are committed as absent and the
    staging copies are purged.  A purge error is therefore explicit and leaves
    the remaining files recoverable inside the reported staging directory.
    """

    plan = build_raw_retention_plan(records, raw_root, as_of=as_of)
    if not apply or plan.blocked:
        return RawRetentionResult(mode="apply" if apply else "dry-run", plan=plan)

    root = Path(plan.raw_root)
    previous_staging = tuple(
        sorted(
            str(path)
            for path in root.glob(f"{_STAGING_PREFIX}*")
            if path.exists()
        )
    )
    if previous_staging:
        return RawRetentionResult(
            mode="apply_failed",
            plan=plan,
            apply_errors=(
                "an earlier retention staging directory requires recovery before apply",
            ),
            staging_directory=previous_staging[0],
        )

    existing_candidates: list[tuple[str, Path, int]] = []
    for relative_path in plan.candidate_paths:
        try:
            target = _target_for_path(root, relative_path)
            if not target.exists():
                continue
            digest_match = _CONTENT_PATH_RE.fullmatch(relative_path)
            if digest_match is None:  # Defensive: the plan already validated this.
                raise ValueError("candidate is not a canonical content-addressed path")
            size = _verify_artifact_payload(target, digest_match.group("digest"))
            existing_candidates.append((relative_path, target, size))
        except (OSError, ValueError) as exc:
            return RawRetentionResult(
                mode="apply_failed",
                plan=plan,
                apply_errors=(f"candidate {relative_path}: {exc}",),
            )

    if not existing_candidates:
        return RawRetentionResult(mode="apply", plan=plan)

    staging = root / f"{_STAGING_PREFIX}{uuid.uuid4().hex}"
    staged: list[tuple[str, Path, Path, int]] = []
    try:
        staging.mkdir(mode=0o700, parents=False, exist_ok=False)
        for relative_path, target, size in existing_candidates:
            staged_target = staging.joinpath(*PurePosixPath(relative_path).parts)
            staged_target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            os.replace(target, staged_target)
            staged.append((relative_path, target, staged_target, size))
    except (OSError, ValueError) as exc:
        rollback_errors: list[str] = []
        for relative_path, target, staged_target, _size in reversed(staged):
            try:
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists():
                    raise FileExistsError("canonical target reappeared during rollback")
                os.replace(staged_target, target)
            except OSError as rollback_exc:
                rollback_errors.append(f"rollback {relative_path}: {rollback_exc}")
        try:
            _remove_empty_staging_tree(staging)
        except OSError as cleanup_exc:
            rollback_errors.append(f"staging cleanup: {cleanup_exc}")
        mode = "rollback_failed" if rollback_errors else "apply_failed"
        return RawRetentionResult(
            mode=mode,
            plan=plan,
            apply_errors=tuple([f"staging failed: {exc}", *rollback_errors]),
            staging_directory=str(staging) if staging.exists() else None,
            cleanup_pending_paths=tuple(
                relative_path
                for relative_path, _target, staged_target, _size in staged
                if staged_target.exists()
            ),
        )

    # Every canonical path is now absent.  From this point a cleanup failure is
    # committed but recoverable from whatever remains under ``staging``.
    committed_paths = tuple(item[0] for item in staged)
    reclaimed_bytes = 0
    apply_errors: list[str] = []
    for relative_path, _target, staged_target, size in staged:
        try:
            staged_target.unlink()
            reclaimed_bytes += size
        except OSError as exc:
            apply_errors.append(f"staging purge {relative_path}: {exc}")
            break

    cleanup_pending = tuple(
        relative_path
        for relative_path, _target, staged_target, _size in staged
        if staged_target.exists()
    )
    if not cleanup_pending:
        try:
            _remove_empty_staging_tree(staging)
        except OSError as exc:
            apply_errors.append(f"staging directory cleanup: {exc}")

    return RawRetentionResult(
        mode="committed_with_cleanup_error" if apply_errors else "apply",
        plan=plan,
        deleted_paths=committed_paths,
        reclaimed_bytes=reclaimed_bytes,
        apply_errors=tuple(apply_errors),
        staging_directory=str(staging) if staging.exists() else None,
        cleanup_pending_paths=cleanup_pending,
    )


def load_snapshot_records(connection: sqlite3.Connection) -> tuple[RawSnapshotRecord, ...]:
    """Load the minimal immutable projection from an already-open transaction."""

    rows = connection.execute(
        """
        SELECT id, dataset, raw_ref_json, observed_at, created_at, content_hash
        FROM research_dataset_snapshots
        WHERE raw_ref_json IS NOT NULL AND raw_ref_json <> ''
        ORDER BY id ASC
        """
    ).fetchall()
    snapshot_hashes: set[str] = set()
    for row in rows:
        content_hash = str(row[5] or "").strip().lower()
        if not _SHA256_RE.fullmatch(content_hash):
            raise ValueError(
                f"record {int(row[0])}: dataset content_hash is invalid"
            )
        snapshot_hashes.add(content_hash)

    latest_bindings: dict[str, datetime] = {}
    event_rows = connection.execute(
        """
        SELECT id, payload_json, created_at
        FROM job_events
        WHERE event_type = ?
        ORDER BY id ASC
        """,
        (_RESEARCH_DATASET_EVENT_TYPE,),
    ).fetchall()
    for event_id, payload_json, created_at in event_rows:
        try:
            payload = json.loads(str(payload_json))
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError(
                f"job event {int(event_id)}: research dataset binding JSON is invalid"
            ) from exc
        if not isinstance(payload, Mapping):
            raise ValueError(
                f"job event {int(event_id)}: research dataset binding must be an object"
            )
        content_hash = str(payload.get("content_hash") or "").strip().lower()
        if not _SHA256_RE.fullmatch(content_hash):
            raise ValueError(
                f"job event {int(event_id)}: research dataset binding hash is invalid"
            )
        if content_hash not in snapshot_hashes:
            # Events can outlive rows only after external database damage; do
            # not let unrelated stale bindings influence retention anchors.
            continue
        bound_at = _utc_datetime(
            created_at,
            field=f"job event {int(event_id)} created_at",
        )
        previous = latest_bindings.get(content_hash)
        if previous is None or bound_at > previous:
            latest_bindings[content_hash] = bound_at

    return tuple(
        RawSnapshotRecord(
            record_id=int(row[0]),
            dataset=str(row[1] or ""),
            raw_ref_json=row[2],
            observed_at=row[3],
            created_at=row[4],
            last_referenced_at=latest_bindings.get(str(row[5]).strip().lower()),
        )
        for row in rows
    )


def _default_database_path() -> Path:
    # Match the application startup contract: a direct maintenance command
    # must see DATABASE_PATH from the repository .env/ENV_FILE as well as from
    # the inherited process environment.
    from src.config import setup_env

    setup_env()
    return Path(os.getenv("DATABASE_PATH", "./data/stock_analysis.db")).expanduser()


def _parse_as_of(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    text = str(value).strip()
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("as_of is not a valid ISO datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("as_of must include Z or an explicit UTC offset")
    return _utc_datetime(value, field="as_of")


def _database_url(database_path: Path) -> str:
    return f"sqlite:///{database_path.as_posix()}"


def main(argv: Optional[Sequence[str]] = None) -> int:
    active_database_path = _default_database_path().resolve(strict=False)
    parser = argparse.ArgumentParser(
        description="Plan or explicitly apply research raw artifact retention",
    )
    parser.add_argument(
        "--database-path",
        type=Path,
        default=active_database_path,
        help=(
            "SQLite database path; defaults to DATABASE_PATH; a different "
            "database is dry-run only"
        ),
    )
    parser.add_argument(
        "--raw-root",
        type=Path,
        help=(
            "Research raw root; defaults to <database-dir>/research/raw; "
            "external roots are dry-run only"
        ),
    )
    parser.add_argument("--as-of", help="UTC/offset ISO timestamp for deterministic planning")
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "maintenance-only deletion at the canonical raw root; stop the "
            "Worker and quiesce database writers first; omitted means dry-run"
        ),
    )
    args = parser.parse_args(argv)

    database_path = args.database_path.expanduser().resolve(strict=False)
    canonical_raw_root = (
        database_path.parent / "research" / "raw"
    ).resolve(strict=False)
    raw_root = (
        args.raw_root.expanduser().resolve(strict=False)
        if args.raw_root is not None
        else canonical_raw_root
    )
    try:
        as_of = _parse_as_of(args.as_of)
    except ValueError as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    if args.apply and not active_database_path.is_file():
        print(
            json.dumps(
                {"error": "active DATABASE_PATH does not exist or is not a file"},
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1
    if args.apply and database_path != active_database_path:
        print(
            json.dumps(
                {
                    "error": (
                        "--apply requires the active DATABASE_PATH loaded from "
                        ".env/ENV_FILE; another database is dry-run only"
                    )
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1
    if args.apply and raw_root != canonical_raw_root:
        print(
            json.dumps(
                {
                    "error": (
                        "--apply requires the canonical raw root adjacent to the "
                        "selected database; external/shared --raw-root is dry-run only"
                    )
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 1
    if not database_path.is_file():
        print(json.dumps({"error": f"SQLite database does not exist: {database_path}"}, ensure_ascii=False))
        return 1

    connection: Optional[sqlite3.Connection] = None
    owner_lock: Optional[TushareWorkerOwnerLock] = None
    try:
        if args.apply:
            owner_lock = TushareWorkerOwnerLock.from_database_url(
                _database_url(database_path)
            ).acquire()
            connection = sqlite3.connect(str(database_path), timeout=30.0)
            connection.execute("PRAGMA busy_timeout = 30000")
            # The owner lock excludes RawArtifactStore activity. BEGIN IMMEDIATE
            # then freezes the selected database reference set for the plan and
            # staging commit built inside both locks.
            connection.execute("BEGIN IMMEDIATE")
        else:
            connection = sqlite3.connect(database_path.as_uri() + "?mode=ro", uri=True, timeout=30.0)
            connection.execute("BEGIN")
        records = load_snapshot_records(connection)
        result = execute_raw_retention(
            records,
            raw_root,
            as_of=as_of,
            apply=bool(args.apply),
        )
        try:
            connection.rollback()
        except sqlite3.Error as exc:
            if not args.apply:
                raise
            committed = bool(result.deleted_paths)
            result = RawRetentionResult(
                mode=(
                    "committed_with_cleanup_error"
                    if committed
                    else (
                        result.mode
                        if result.mode in {"apply_failed", "rollback_failed"}
                        else "apply_failed"
                    )
                ),
                plan=result.plan,
                deleted_paths=result.deleted_paths,
                reclaimed_bytes=result.reclaimed_bytes,
                apply_errors=(
                    *result.apply_errors,
                    f"database transaction release after filesystem staging: {exc}",
                ),
                staging_directory=result.staging_directory,
                cleanup_pending_paths=result.cleanup_pending_paths,
            )
    except (
        OSError,
        sqlite3.Error,
        TushareWorkerOwnerError,
        TypeError,
        ValueError,
    ) as exc:
        if connection is not None:
            try:
                connection.rollback()
            except sqlite3.Error:
                pass
        print(json.dumps({"error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    finally:
        if connection is not None:
            connection.close()
        if owner_lock is not None:
            owner_lock.release()

    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
    if result.plan.blocked:
        return 2
    if result.apply_errors:
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through ``main`` tests
    raise SystemExit(main())


__all__ = [
    "MARKET_RAW_DATASETS",
    "MARKET_RAW_RETENTION_DAYS",
    "ORPHAN_RAW_GRACE_DAYS",
    "PERMANENT_RAW_DATASETS",
    "RawRetentionPlan",
    "RawRetentionResult",
    "RawSnapshotRecord",
    "SEARCH_NEWS_RAW_DATASETS",
    "SEARCH_NEWS_RAW_RETENTION_DAYS",
    "build_raw_retention_plan",
    "execute_raw_retention",
    "load_snapshot_records",
    "main",
    "retention_days_for_dataset",
]
