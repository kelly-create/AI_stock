# -*- coding: utf-8 -*-
"""Ordered, single-writer database migrations for the DSA SQLite database.

The application keeps its historical automatic initialization path for
backward compatibility, but all schema writes now use the same process-shared
lock as the explicit ``--apply`` command.  Production deployments can run the
CLI before starting API/worker/scheduler processes and use ``--check`` as a
strictly read-only preflight.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import sqlite3
import threading
import time
from typing import Callable, Iterator, Optional, Sequence
from urllib.parse import quote

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, make_url

try:  # pragma: no cover - platform-specific import
    import fcntl
except ImportError:  # pragma: no cover - Windows
    fcntl = None

try:  # pragma: no cover - platform-specific import
    import msvcrt
except ImportError:  # pragma: no cover - POSIX
    msvcrt = None


BASELINE_SCHEMA_VERSION = "2026-06-05-create-all-baseline"
PR0_CONVERGENCE_SCHEMA_VERSION = "2026-08-07-pr0-schema-convergence"
PR1_DURABLE_JOBS_SCHEMA_VERSION = "2026-08-08-pr1-durable-jobs"
PR2_RESEARCH_DATA_SCHEMA_VERSION = "2026-08-08-pr2-research-data"
PR3_RESEARCH_EVIDENCE_SCHEMA_VERSION = "2026-08-08-pr3-research-evidence"
PR4_RESEARCH_DEBATE_SCHEMA_VERSION = "2026-08-08-pr4-research-debate"
PERSONAL_RESEARCH_POLICY_SCHEMA_VERSION = "2026-08-10-personal-research-policy"
PERSONAL_RESEARCH_SKILLS_SCHEMA_VERSION = "2026-08-10-personal-research-skills"
DECISION_OUTCOME_V2_SCHEMA_VERSION = "2026-08-10-personal-research-v2-outcomes"
PERSONAL_RESEARCH_POLICY_CONTEXT_SCHEMA_VERSION = (
    "2026-08-10-personal-research-v3-policy-context"
)


class MigrationError(RuntimeError):
    """Base error for migration discovery or execution failures."""


class MigrationLockTimeout(MigrationError):
    """Raised when another process keeps the migration writer lock."""


@dataclass(frozen=True)
class Migration:
    """One immutable, ordered database migration."""

    version: str
    description: str
    apply: Callable[[Engine], None]


@dataclass(frozen=True)
class MigrationState:
    """Read-only view of the database migration state."""

    database_url: str
    current_version: Optional[str]
    latest_version: str
    applied_versions: tuple[str, ...]
    pending_versions: tuple[str, ...]
    unknown_versions: tuple[str, ...]
    is_current: bool
    is_compatible: bool
    error: Optional[str] = None


def _create_baseline_schema(engine: Engine) -> None:
    # Lazy import avoids a module cycle: storage imports the migration runner so
    # its legacy automatic initialization can share this writer lock.
    from src.storage import Base

    Base.metadata.create_all(bind=engine)


def _converge_pr0_storage_schema(engine: Engine) -> None:
    """Run every pre-framework schema repair through the shared storage adapter."""

    from src.storage import run_storage_schema_convergence

    run_storage_schema_convergence(engine)


def _upgrade_pr1_durable_jobs_schema(engine: Engine) -> None:
    """Install the durable job, event, outbox, and provider-health contract."""

    from src.storage import run_pr1_durable_jobs_schema_upgrade

    run_pr1_durable_jobs_schema_upgrade(engine)


def _upgrade_pr2_research_data_schema(engine: Engine) -> None:
    """Install the immutable research dataset/factor/pack snapshot contract."""

    from src.storage import run_pr2_research_schema_upgrade

    run_pr2_research_schema_upgrade(engine)


def _upgrade_pr3_research_evidence_schema(engine: Engine) -> None:
    """Install immutable evidence snapshots and research-pack references."""

    from src.storage import run_pr3_research_evidence_schema_upgrade

    run_pr3_research_evidence_schema_upgrade(engine)


def _upgrade_pr4_research_debate_schema(engine: Engine) -> None:
    """Install immutable debate requests, turns, snapshots, and pack links."""

    from src.storage import run_pr4_research_debate_schema_upgrade

    run_pr4_research_debate_schema_upgrade(engine)


def _upgrade_personal_research_policy_schema(engine: Engine) -> None:
    """Install original-plan PR3 watchlist, reconciliation, policy, and budget storage."""

    from src.storage import run_personal_research_policy_schema_upgrade

    run_personal_research_policy_schema_upgrade(engine)


def _upgrade_personal_research_skills_schema(engine: Engine) -> None:
    """Install immutable personal Skill, Debate-review, and Thesis storage."""

    from src.storage import run_personal_research_skills_schema_upgrade

    run_personal_research_skills_schema_upgrade(engine)


def _upgrade_decision_outcome_v2_schema(engine: Engine) -> None:
    """Install independent immutable personal-research outcome observations."""

    from src.storage import run_decision_outcome_v2_schema_upgrade

    run_decision_outcome_v2_schema_upgrade(engine)


def _upgrade_personal_research_policy_context_schema(engine: Engine) -> None:
    """Seal replayable Portfolio Policy context and immutable audit rows."""

    from src.storage import run_personal_research_policy_context_schema_upgrade

    run_personal_research_policy_context_schema_upgrade(engine)


MIGRATIONS: tuple[Migration, ...] = (
    Migration(
        version=BASELINE_SCHEMA_VERSION,
        description="Baseline schema created through SQLAlchemy metadata.create_all",
        apply=_create_baseline_schema,
    ),
    Migration(
        version=PR0_CONVERGENCE_SCHEMA_VERSION,
        description=(
            "Converge ORM tables, legacy telemetry columns, decision profiles, "
            "and intelligence scope/index contracts"
        ),
        apply=_converge_pr0_storage_schema,
    ),
    Migration(
        version=PR1_DURABLE_JOBS_SCHEMA_VERSION,
        description=(
            "Add durable analysis jobs, events, notification outbox, provider "
            "health, and cross-record idempotency/trace columns"
        ),
        apply=_upgrade_pr1_durable_jobs_schema,
    ),
    Migration(
        version=PR2_RESEARCH_DATA_SCHEMA_VERSION,
        description=(
            "Add immutable research dataset, deterministic factor, and "
            "versioned research pack snapshots"
        ),
        apply=_upgrade_pr2_research_data_schema,
    ),
    Migration(
        version=PR3_RESEARCH_EVIDENCE_SCHEMA_VERSION,
        description=(
            "Add immutable research evidence snapshots and bind them to "
            "versioned research packs"
        ),
        apply=_upgrade_pr3_research_evidence_schema,
    ),
    Migration(
        version=PR4_RESEARCH_DEBATE_SCHEMA_VERSION,
        description=(
            "Add immutable research debate requests, turns, snapshots, and "
            "bind final debates to versioned research packs"
        ),
        apply=_upgrade_pr4_research_debate_schema,
    ),
    Migration(
        version=PERSONAL_RESEARCH_POLICY_SCHEMA_VERSION,
        description=(
            "Add enhanced research watchlist metadata, append-only portfolio "
            "reconciliation, deterministic policy audits, research budgets, "
            "and the extended DecisionSignal contract"
        ),
        apply=_upgrade_personal_research_policy_schema,
    ),
    Migration(
        version=PERSONAL_RESEARCH_SKILLS_SCHEMA_VERSION,
        description=(
            "Add immutable personal Skill executions, deterministic Debate "
            "reviews, and versioned Research Theses"
        ),
        apply=_upgrade_personal_research_skills_schema,
    ),
    Migration(
        version=DECISION_OUTCOME_V2_SCHEMA_VERSION,
        description=(
            "Add immutable Decision Outcome v2 observations with frozen "
            "signal, policy, execution, benchmark, and dataset lineage"
        ),
        apply=_upgrade_decision_outcome_v2_schema,
    ),
    Migration(
        version=PERSONAL_RESEARCH_POLICY_CONTEXT_SCHEMA_VERSION,
        description=(
            "Add canonical Portfolio Policy context payloads and database-level "
            "immutability for policy evaluation audits"
        ),
        apply=_upgrade_personal_research_policy_context_schema,
    ),
)
LATEST_SCHEMA_VERSION = MIGRATIONS[-1].version


_SCHEMA_MIGRATIONS_DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version VARCHAR(64) NOT NULL PRIMARY KEY,
    description VARCHAR(255) NOT NULL,
    applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""
_SCHEMA_MIGRATIONS_INDEX_DDL = (
    "CREATE INDEX IF NOT EXISTS ix_schema_migrations_applied_at "
    "ON schema_migrations (applied_at)"
)

_thread_locks_guard = threading.Lock()
_thread_locks: dict[str, threading.Lock] = {}


def _validate_registry(migrations: Sequence[Migration]) -> None:
    versions = [migration.version for migration in migrations]
    if not versions:
        raise MigrationError("Migration registry must not be empty")
    if len(versions) != len(set(versions)):
        raise MigrationError("Migration registry contains duplicate versions")
    if versions != sorted(versions):
        raise MigrationError("Migration registry must be ordered by version")


def _sqlite_database_path(database_url: str) -> Optional[Path]:
    url = make_url(database_url)
    if url.get_backend_name() != "sqlite":
        raise MigrationError("Only SQLite migrations are supported")
    database = url.database
    if not database or database == ":memory:":
        return None
    # ``strict=False`` resolves every existing symlink component while still
    # supporting a database file (or trailing parent directory) that has not
    # been created yet.  Lock aliases must collapse to one path before either
    # the lock file or SQLite database exists.
    return Path(database).expanduser().resolve(strict=False)


def _lock_key(database_url: str) -> str:
    database_path = _sqlite_database_path(database_url)
    if database_path is None:
        return f"memory:{database_url}"
    return str(database_path)


def _get_thread_lock(key: str) -> threading.Lock:
    with _thread_locks_guard:
        return _thread_locks.setdefault(key, threading.Lock())


def _try_lock_file(handle) -> bool:
    if fcntl is not None:  # POSIX advisory lock
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except (BlockingIOError, OSError):
            return False
    if msvcrt is not None:  # Windows byte-range lock
        try:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False
    raise MigrationError("No supported process-shared file locking primitive is available")


def _unlock_file(handle) -> None:
    if fcntl is not None:  # pragma: no branch - platform-specific
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    elif msvcrt is not None:  # pragma: no branch - platform-specific
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)


@contextmanager
def migration_writer_lock(
    database_url: str,
    *,
    timeout_seconds: float = 30.0,
) -> Iterator[None]:
    """Serialize schema writes across threads and same-host processes.

    The OS releases the advisory lock when a process exits, so a crash cannot
    leave behind a stale owner.  The lock file itself intentionally persists.
    """

    key = _lock_key(database_url)
    thread_lock = _get_thread_lock(key)
    if not thread_lock.acquire(timeout=max(0.0, timeout_seconds)):
        raise MigrationLockTimeout(
            f"Timed out waiting for migration lock after {timeout_seconds:.1f}s"
        )

    handle = None
    file_locked = False
    try:
        database_path = _sqlite_database_path(database_url)
        if database_path is None:
            yield
            return

        database_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = Path(f"{database_path}.migration.lock")
        handle = open(lock_path, "a+b")

        deadline = time.monotonic() + max(0.0, timeout_seconds)
        while not _try_lock_file(handle):
            if time.monotonic() >= deadline:
                raise MigrationLockTimeout(
                    f"Timed out waiting for migration lock {lock_path} "
                    f"after {timeout_seconds:.1f}s"
                )
            time.sleep(0.05)
        file_locked = True

        # Initialize the byte used by ``msvcrt.locking`` only after this
        # process owns the lock.  Initializing it before locking lets two
        # Windows processes both observe an empty file; once one process locks
        # byte zero, the other's buffered flush can fail with PermissionError
        # instead of waiting.  Both flock and msvcrt can lock an empty file, so
        # this also safely repairs a zero-byte file left by an interrupted
        # first startup.
        if lock_path.stat().st_size == 0:
            handle.seek(0)
            handle.write(b"0")
            handle.flush()
        yield
    finally:
        if handle is not None:
            try:
                if file_locked:
                    _unlock_file(handle)
            finally:
                handle.close()
        thread_lock.release()


def _state_from_versions(
    database_url: str,
    applied_from_database: Sequence[str],
    *,
    migrations: Sequence[Migration],
    error: Optional[str] = None,
) -> MigrationState:
    _validate_registry(migrations)
    known_versions = tuple(migration.version for migration in migrations)
    applied_set = set(applied_from_database)
    applied_versions = tuple(version for version in known_versions if version in applied_set)
    pending_versions = tuple(version for version in known_versions if version not in applied_set)
    unknown_versions = tuple(sorted(applied_set.difference(known_versions)))

    gap_detected = False
    seen_pending = False
    for version in known_versions:
        if version not in applied_set:
            seen_pending = True
        elif seen_pending:
            gap_detected = True
            break

    compatibility_error = error
    if unknown_versions and compatibility_error is None:
        compatibility_error = (
            "Database contains migration versions unknown to this build: "
            + ", ".join(unknown_versions)
        )
    if gap_detected and compatibility_error is None:
        compatibility_error = "Database migration history is out of order"

    is_compatible = compatibility_error is None and not unknown_versions and not gap_detected
    is_current = is_compatible and not pending_versions
    current_version = applied_versions[-1] if applied_versions else None
    return MigrationState(
        database_url=database_url,
        current_version=current_version,
        latest_version=known_versions[-1],
        applied_versions=applied_versions,
        pending_versions=pending_versions,
        unknown_versions=unknown_versions,
        is_current=is_current,
        is_compatible=is_compatible,
        error=compatibility_error,
    )


def check_migration_state(
    database_url: str,
    *,
    migrations: Sequence[Migration] = MIGRATIONS,
) -> MigrationState:
    """Inspect migration state without creating a DB, table, or manager."""

    _validate_registry(migrations)
    try:
        database_path = _sqlite_database_path(database_url)
    except Exception as exc:
        return _state_from_versions(
            database_url,
            (),
            migrations=migrations,
            error=str(exc),
        )

    if database_path is None or not database_path.exists():
        return _state_from_versions(database_url, (), migrations=migrations)

    try:
        database_uri = f"file:{quote(database_path.as_posix(), safe='/:')}?mode=ro"
        with sqlite3.connect(database_uri, uri=True, timeout=2.0) as connection:
            table_exists = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations'"
            ).fetchone()
            if table_exists is None:
                applied = ()
            else:
                applied = tuple(
                    row[0]
                    for row in connection.execute(
                        "SELECT version FROM schema_migrations"
                    ).fetchall()
                )
        return _state_from_versions(database_url, applied, migrations=migrations)
    except Exception as exc:
        return _state_from_versions(
            database_url,
            (),
            migrations=migrations,
            error=f"Failed to inspect migration state: {exc}",
        )


def apply_migrations_locked(
    engine: Engine,
    *,
    migrations: Sequence[Migration] = MIGRATIONS,
) -> tuple[str, ...]:
    """Apply pending migrations; caller must hold ``migration_writer_lock``."""

    _validate_registry(migrations)
    applied_now: list[str] = []
    with engine.begin() as connection:
        connection.exec_driver_sql(_SCHEMA_MIGRATIONS_DDL)
        connection.exec_driver_sql(_SCHEMA_MIGRATIONS_INDEX_DDL)
        applied = {
            row[0]
            for row in connection.exec_driver_sql(
                "SELECT version FROM schema_migrations"
            ).all()
        }
        known = {migration.version for migration in migrations}
        unknown = sorted(applied.difference(known))
        if unknown:
            raise MigrationError(
                "Database contains migration versions unknown to this build: "
                + ", ".join(unknown)
            )

        seen_pending = False
        pending: list[Migration] = []
        for migration in migrations:
            if migration.version in applied:
                if seen_pending:
                    raise MigrationError("Database migration history is out of order")
                continue
            seen_pending = True
            pending.append(migration)

    # Historical convergence uses several independently idempotent DDL/data
    # transactions.  Keep the process-shared writer lock for the whole loop and
    # record each version only after its callback completes.  A crash before the
    # marker therefore resumes the same migration safely on the next --apply.
    for migration in pending:
        migration.apply(engine)
        with engine.begin() as connection:
            connection.exec_driver_sql(
                "INSERT INTO schema_migrations (version, description, applied_at) "
                "VALUES (?, ?, CURRENT_TIMESTAMP)",
                (migration.version, migration.description),
            )
        applied_now.append(migration.version)
    return tuple(applied_now)


def apply_migrations(
    database_url: str,
    *,
    lock_timeout_seconds: float = 30.0,
    migrations: Sequence[Migration] = MIGRATIONS,
) -> MigrationState:
    """Apply every pending migration under the shared single-writer lock."""

    database_path = _sqlite_database_path(database_url)
    if database_path is not None:
        database_path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(database_url, pool_pre_ping=True)
    try:
        with migration_writer_lock(
            database_url,
            timeout_seconds=lock_timeout_seconds,
        ):
            apply_migrations_locked(engine, migrations=migrations)
        if database_path is None:
            with engine.connect() as connection:
                applied = tuple(
                    row[0]
                    for row in connection.exec_driver_sql(
                        "SELECT version FROM schema_migrations"
                    ).all()
                )
            state = _state_from_versions(
                database_url,
                applied,
                migrations=migrations,
            )
        else:
            state = check_migration_state(database_url, migrations=migrations)
        if not state.is_current:
            raise MigrationError(state.error or "Migration apply did not reach the latest version")
        return state
    finally:
        engine.dispose()


def _default_database_url() -> str:
    from src.config import setup_env

    setup_env()
    database_path = Path(
        os.getenv("DATABASE_PATH", "./data/stock_analysis.db")
    ).expanduser().absolute()
    return f"sqlite:///{database_path.as_posix()}"


def _render_state(state: MigrationState) -> str:
    return json.dumps(asdict(state), ensure_ascii=False, indent=2)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Check or apply DSA database migrations")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--check", action="store_true", help="read-only migration preflight")
    action.add_argument("--apply", action="store_true", help="apply pending migrations")
    parser.add_argument(
        "--database-url",
        help="SQLAlchemy SQLite URL; defaults to DATABASE_PATH from runtime config",
    )
    parser.add_argument("--lock-timeout", type=float, default=30.0)
    args = parser.parse_args(argv)
    database_url = args.database_url or _default_database_url()

    if args.check:
        state = check_migration_state(database_url)
        print(_render_state(state))
        if not state.is_compatible:
            return 3
        return 0 if state.is_current else 2

    try:
        state = apply_migrations(
            database_url,
            lock_timeout_seconds=args.lock_timeout,
        )
    except Exception as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False, indent=2))
        return 1
    print(_render_state(state))
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through CLI tests
    raise SystemExit(main())
