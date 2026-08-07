"""Deterministic tests for the non-mutating readiness service."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from src.migrations import MIGRATIONS
from src.services.readiness_service import ReadinessCheckResult, ReadinessService


def _create_probe_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "CREATE TABLE schema_migrations ("
            "version VARCHAR(64) PRIMARY KEY, "
            "description VARCHAR(255) NOT NULL, "
            "applied_at DATETIME NOT NULL)"
        )
        connection.commit()
    finally:
        connection.close()


def _current_migration(_database_url: str) -> ReadinessCheckResult:
    return ReadinessCheckResult("ready", "migration_current")


def test_readiness_checks_sqlite_read_and_rolled_back_write(tmp_path: Path) -> None:
    database_path = tmp_path / "ready.db"
    _create_probe_database(database_path)
    service = ReadinessService(
        database_path_provider=lambda: database_path,
        migration_checker=_current_migration,
    )

    report = service.check()

    assert report.ready is True
    assert report.checks["migrations"].status == "ready"
    assert report.checks["database_read"].status == "ready"
    assert report.checks["database_write"].status == "ready"
    assert report.checks["worker_heartbeat"].status == "skipped"
    with sqlite3.connect(database_path) as connection:
        probe_rows = connection.execute(
            "SELECT version FROM schema_migrations WHERE version LIKE '__readiness_probe__%'"
        ).fetchall()
    assert probe_rows == []


def test_readiness_uses_read_only_migration_runner_state(tmp_path: Path) -> None:
    database_path = tmp_path / "migration-current.db"
    _create_probe_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.executemany(
            "INSERT INTO schema_migrations (version, description, applied_at) "
            "VALUES (?, ?, CURRENT_TIMESTAMP)",
            [
                (migration.version, migration.description)
                for migration in MIGRATIONS
            ],
        )

    report = ReadinessService(
        database_path_provider=lambda: database_path,
    ).check()

    assert report.ready is True
    assert report.checks["migrations"].detail == "migration_current"


def test_unknown_migration_version_blocks_readiness_without_exposing_it(tmp_path: Path) -> None:
    database_path = tmp_path / "migration-incompatible.db"
    _create_probe_database(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "INSERT INTO schema_migrations (version, description, applied_at) "
            "VALUES (?, ?, CURRENT_TIMESTAMP)",
            ("future-secret-version", "future"),
        )

    report = ReadinessService(
        database_path_provider=lambda: database_path,
    ).check()

    assert report.ready is False
    assert report.checks["migrations"].detail == "migration_incompatible"
    assert "future-secret-version" not in report.checks["migrations"].detail


def test_readiness_does_not_create_a_missing_database(tmp_path: Path) -> None:
    database_path = tmp_path / "missing.db"
    service = ReadinessService(
        database_path_provider=lambda: database_path,
        migration_checker=lambda _database_url: ReadinessCheckResult(
            "not_ready",
            "migration_pending",
        ),
    )

    report = service.check()

    assert report.ready is False
    assert report.checks["migrations"].detail == "migration_pending"
    assert report.checks["database_read"].detail == "database_unavailable"
    assert report.checks["database_write"].detail == "database_unavailable"
    assert not database_path.exists()


def test_pending_migration_blocks_readiness_while_database_probes_pass(tmp_path: Path) -> None:
    database_path = tmp_path / "pending.db"
    _create_probe_database(database_path)
    service = ReadinessService(
        database_path_provider=lambda: database_path,
        migration_checker=lambda _database_url: ReadinessCheckResult(
            "not_ready",
            "migration_pending",
        ),
    )

    report = service.check()

    assert report.ready is False
    assert report.checks["migrations"].detail == "migration_pending"
    assert report.checks["database_read"].status == "ready"
    assert report.checks["database_write"].status == "ready"


def test_worker_heartbeat_only_blocks_when_explicitly_required(tmp_path: Path) -> None:
    database_path = tmp_path / "worker.db"
    _create_probe_database(database_path)

    def stale_worker() -> ReadinessCheckResult:
        return ReadinessCheckResult("not_ready", "worker_heartbeat_stale")

    optional_report = ReadinessService(
        database_path_provider=lambda: database_path,
        migration_checker=_current_migration,
        worker_heartbeat_checker=stale_worker,
        require_worker_heartbeat=False,
    ).check()
    required_report = ReadinessService(
        database_path_provider=lambda: database_path,
        migration_checker=_current_migration,
        worker_heartbeat_checker=stale_worker,
        require_worker_heartbeat=True,
    ).check()

    assert optional_report.ready is True
    assert optional_report.checks["worker_heartbeat"].required is False
    assert required_report.ready is False
    assert required_report.checks["worker_heartbeat"].required is True


class _QueryOnlyReadinessService(ReadinessService):
    def _connect_existing_database(self, database_path: Path) -> sqlite3.Connection:
        connection = super()._connect_existing_database(database_path)
        connection.execute("PRAGMA query_only = ON")
        return connection


def test_readable_but_unwritable_database_is_not_ready(tmp_path: Path) -> None:
    database_path = tmp_path / "query-only.db"
    _create_probe_database(database_path)
    service = _QueryOnlyReadinessService(
        database_path_provider=lambda: database_path,
        migration_checker=_current_migration,
    )

    report = service.check()

    assert report.ready is False
    assert report.checks["database_read"].status == "ready"
    assert report.checks["database_write"].detail == "database_write_failed"
