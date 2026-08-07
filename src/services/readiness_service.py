# -*- coding: utf-8 -*-
"""Side-effect-free application readiness checks.

Readiness deliberately opens SQLite independently from ``DatabaseManager``.
Constructing ``DatabaseManager`` currently creates and repairs schema objects,
which must not happen from a health probe once migrations are an explicit
deployment step.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Mapping, Optional
from urllib.parse import quote
from uuid import uuid4

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReadinessCheckResult:
    """One stable, non-sensitive readiness check result."""

    status: str
    detail: str
    required: bool = True

    def __post_init__(self) -> None:
        if self.status not in {"ready", "not_ready", "skipped"}:
            raise ValueError(f"Unsupported readiness status: {self.status}")

    @property
    def ready(self) -> bool:
        return self.status == "ready" or not self.required


@dataclass(frozen=True)
class ReadinessReport:
    """Aggregate readiness state returned to the API adapter."""

    checks: Mapping[str, ReadinessCheckResult]

    @property
    def ready(self) -> bool:
        return all(check.ready for check in self.checks.values())


MigrationChecker = Callable[[str], ReadinessCheckResult]
WorkerHeartbeatChecker = Callable[[], ReadinessCheckResult]
DatabasePathProvider = Callable[[], Path]


def _configured_database_path() -> Path:
    from src.config import get_config

    return Path(get_config().database_path).expanduser().resolve()


def _migration_state_check(database_url: str) -> ReadinessCheckResult:
    """Adapt the read-only migration runner to the public health contract."""

    from src.migrations import check_migration_state

    state = check_migration_state(database_url)
    if not state.is_compatible or state.error is not None:
        return ReadinessCheckResult("not_ready", "migration_incompatible")
    if not state.is_current:
        return ReadinessCheckResult("not_ready", "migration_pending")
    return ReadinessCheckResult("ready", "migration_current")


class ReadinessService:
    """Check migration state, SQLite access, and an optional worker heartbeat."""

    def __init__(
        self,
        *,
        database_path_provider: DatabasePathProvider = _configured_database_path,
        migration_checker: MigrationChecker = _migration_state_check,
        worker_heartbeat_checker: Optional[WorkerHeartbeatChecker] = None,
        require_worker_heartbeat: bool = False,
        sqlite_timeout_seconds: float = 2.0,
    ) -> None:
        self._database_path_provider = database_path_provider
        self._migration_checker = migration_checker
        self._worker_heartbeat_checker = worker_heartbeat_checker
        self._require_worker_heartbeat = require_worker_heartbeat
        self._sqlite_timeout_seconds = max(0.1, float(sqlite_timeout_seconds))

    def check(self) -> ReadinessReport:
        checks: dict[str, ReadinessCheckResult] = {}
        connection: Optional[sqlite3.Connection] = None

        try:
            database_path = self._database_path_provider()
        except Exception as exc:  # noqa: BLE001 - readiness must report unavailable.
            logger.warning("Readiness database configuration failed: %s", exc)
            unavailable = ReadinessCheckResult("not_ready", "database_unavailable")
            checks.update(
                {
                    "migrations": unavailable,
                    "database_read": unavailable,
                    "database_write": unavailable,
                }
            )
        else:
            database_url = f"sqlite:///{database_path.as_posix()}"
            checks["migrations"] = self._run_migration_check(database_url)
            try:
                connection = self._connect_existing_database(database_path)
            except Exception as exc:  # noqa: BLE001 - readiness must report unavailable.
                logger.warning("Readiness database connection failed: %s", exc)
                unavailable = ReadinessCheckResult("not_ready", "database_unavailable")
                checks["database_read"] = unavailable
                checks["database_write"] = unavailable
            else:
                checks["database_read"] = self._run_read_check(connection)
                checks["database_write"] = self._run_write_check(connection)
        finally:
            if connection is not None:
                try:
                    if connection.in_transaction:
                        connection.rollback()
                finally:
                    connection.close()

        checks["worker_heartbeat"] = self._run_worker_heartbeat_check()
        return ReadinessReport(checks=checks)

    def _connect_existing_database(self, database_path: Path) -> sqlite3.Connection:
        if not database_path.is_file():
            raise FileNotFoundError("configured SQLite database does not exist")
        database_uri = f"file:{quote(database_path.as_posix(), safe='/:')}?mode=rw"
        return sqlite3.connect(
            database_uri,
            uri=True,
            timeout=self._sqlite_timeout_seconds,
            isolation_level=None,
        )

    def _run_migration_check(self, database_url: str) -> ReadinessCheckResult:
        try:
            result = self._migration_checker(database_url)
            if not isinstance(result, ReadinessCheckResult):
                raise TypeError("migration checker returned an invalid result")
            return result
        except Exception as exc:  # noqa: BLE001 - readiness must report unavailable.
            logger.warning("Readiness migration check failed: %s", exc)
            return ReadinessCheckResult("not_ready", "migration_check_failed")

    @staticmethod
    def _run_read_check(connection: sqlite3.Connection) -> ReadinessCheckResult:
        try:
            row = connection.execute("SELECT 1").fetchone()
            if row != (1,):
                raise RuntimeError("SQLite read probe returned an unexpected result")
            return ReadinessCheckResult("ready", "database_readable")
        except Exception as exc:  # noqa: BLE001 - readiness must report unavailable.
            logger.warning("Readiness database read failed: %s", exc)
            return ReadinessCheckResult("not_ready", "database_read_failed")

    @staticmethod
    def _run_write_check(connection: sqlite3.Connection) -> ReadinessCheckResult:
        probe_version = f"__readiness_probe__{uuid4().hex}"
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT INTO schema_migrations (version, description, applied_at) "
                "VALUES (?, ?, ?)",
                (
                    probe_version,
                    "Transient readiness write probe; transaction is rolled back",
                    datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
                ),
            )
            connection.rollback()
            return ReadinessCheckResult("ready", "database_writable")
        except Exception as exc:  # noqa: BLE001 - readiness must report unavailable.
            if connection.in_transaction:
                connection.rollback()
            logger.warning("Readiness database write failed: %s", exc)
            return ReadinessCheckResult("not_ready", "database_write_failed")

    def _run_worker_heartbeat_check(self) -> ReadinessCheckResult:
        if self._worker_heartbeat_checker is None:
            if self._require_worker_heartbeat:
                return ReadinessCheckResult("not_ready", "worker_heartbeat_unavailable")
            return ReadinessCheckResult(
                "skipped",
                "worker_heartbeat_not_required",
                required=False,
            )

        try:
            result = self._worker_heartbeat_checker()
            if not isinstance(result, ReadinessCheckResult):
                raise TypeError("worker heartbeat checker returned an invalid result")
            return ReadinessCheckResult(
                result.status,
                result.detail,
                required=self._require_worker_heartbeat,
            )
        except Exception as exc:  # noqa: BLE001 - readiness must report unavailable.
            logger.warning("Readiness worker heartbeat check failed: %s", exc)
            return ReadinessCheckResult(
                "not_ready",
                "worker_heartbeat_check_failed",
                required=self._require_worker_heartbeat,
            )
