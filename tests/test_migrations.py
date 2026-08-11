import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.migrations import (
    BASELINE_SCHEMA_VERSION,
    DECISION_OUTCOME_V2_SCHEMA_VERSION,
    MIGRATIONS,
    PR0_CONVERGENCE_SCHEMA_VERSION,
    PR1_DURABLE_JOBS_SCHEMA_VERSION,
    PR2_RESEARCH_DATA_SCHEMA_VERSION,
    PR3_RESEARCH_EVIDENCE_SCHEMA_VERSION,
    PR4_RESEARCH_DEBATE_SCHEMA_VERSION,
    PERSONAL_RESEARCH_POLICY_CONTEXT_SCHEMA_VERSION,
    PERSONAL_RESEARCH_POLICY_SCHEMA_VERSION,
    PERSONAL_RESEARCH_SKILLS_SCHEMA_VERSION,
    Migration,
    MigrationError,
    _sqlite_database_path,
    apply_migrations,
    check_migration_state,
    main,
)


def _sqlite_url(path: Path) -> str:
    return f"sqlite:///{path.as_posix()}"


def _migrations_through(version: str) -> tuple[Migration, ...]:
    """Return the stable migration prefix ending at ``version``.

    Tests that reconstruct a historical schema must not depend on the number
    of migrations appended after that schema.  Version-addressed prefixes keep
    those fixtures honest as the rollout advances.
    """

    for index, migration in enumerate(MIGRATIONS):
        if migration.version == version:
            return MIGRATIONS[: index + 1]
    raise AssertionError(f"unknown migration version: {version}")


_PR1_LLM_USAGE_AUDIT_COLUMN_TYPES = {
    "job_id": "VARCHAR(64)",
    "stage": "VARCHAR(64)",
    "trace_id": "VARCHAR(64)",
    "prompt_version": "VARCHAR(64)",
    "snapshot_hash": "VARCHAR(128)",
    "latency_ms": "INTEGER",
    "status": "VARCHAR(32)",
    "error_code": "VARCHAR(64)",
    "error_message_sanitized": "TEXT",
    "estimated_cost_usd": "FLOAT",
    "cost_source": "VARCHAR(32)",
    "attempt_no": "INTEGER",
}
_PR1_LLM_USAGE_AUDIT_COLUMNS = set(_PR1_LLM_USAGE_AUDIT_COLUMN_TYPES)
_PR1_LLM_USAGE_FORBIDDEN_GENERIC_COLUMNS = {
    "latency",
    "error",
    "cost",
    "attempt",
}
_PR1_LLM_USAGE_AUDIT_INDEXES = {
    "ix_llm_usage_job_stage_called_at": ("job_id", "stage", "called_at"),
    "ix_llm_usage_trace_called_at": ("trace_id", "called_at"),
    "ix_llm_usage_status_called_at": ("status", "called_at"),
}


def _llm_usage_audit_column_contract(
    connection: sqlite3.Connection,
) -> dict[str, tuple[str, bool]]:
    return {
        row[1]: (row[2], bool(row[3]))
        for row in connection.execute("PRAGMA table_info('llm_usage')")
        if row[1] in _PR1_LLM_USAGE_AUDIT_COLUMNS
    }


def _create_pr0_shaped_schema(
    database_path: Path,
    *,
    include_migration_history: bool = True,
) -> None:
    """Create the smallest pre-PR1 shape needed to exercise only PR1 DDL."""

    with sqlite3.connect(database_path) as connection:
        migration_sql = ""
        if include_migration_history:
            migration_sql = f"""
            CREATE TABLE schema_migrations (
                version VARCHAR(64) PRIMARY KEY,
                description VARCHAR(255) NOT NULL,
                applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            INSERT INTO schema_migrations (version, description) VALUES
                ('{BASELINE_SCHEMA_VERSION}', 'baseline'),
                ('{PR0_CONVERGENCE_SCHEMA_VERSION}', 'pr0 convergence');
            """
        connection.executescript(
            f"""
            {migration_sql}
            CREATE TABLE llm_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                called_at DATETIME
            );
            CREATE TABLE analysis_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code VARCHAR(10) NOT NULL,
                report_type VARCHAR(16)
            );
            CREATE TABLE decision_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT
            );
            INSERT INTO llm_usage (called_at) VALUES (CURRENT_TIMESTAMP);
            INSERT INTO analysis_history (code, report_type)
                VALUES ('600519', 'detailed');
            INSERT INTO decision_signals DEFAULT VALUES;
            """
        )


_SUBPROCESS_MIGRATION_SCRIPT = """
import os
from pathlib import Path
import sys
import time

from src.migrations import Migration, apply_migrations

database_url, start_raw, sentinel_raw, ready_raw = sys.argv[1:]
start_path = Path(start_raw)
sentinel_path = Path(sentinel_raw)
Path(ready_raw).write_text("ready", encoding="utf-8")
deadline = time.monotonic() + 15
while not start_path.exists():
    if time.monotonic() >= deadline:
        raise RuntimeError("timed out waiting for subprocess start barrier")
    time.sleep(0.01)

def apply_once(engine):
    fd = os.open(
        sentinel_path,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY,
    )
    try:
        os.write(fd, str(os.getpid()).encode("ascii"))
        time.sleep(0.4)
        with engine.begin() as connection:
            connection.exec_driver_sql(
                "CREATE TABLE subprocess_probe "
                "(id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
            )
            connection.exec_driver_sql(
                "INSERT INTO subprocess_probe (id, value) VALUES (1, 'once')"
            )
    finally:
        os.close(fd)
        sentinel_path.unlink(missing_ok=True)

apply_migrations(
    database_url,
    migrations=(Migration("001-subprocess", "subprocess writer probe", apply_once),),
    lock_timeout_seconds=10,
)
"""


_SUBPROCESS_EMPTY_LOCK_HOLDER = """
import os
from pathlib import Path
import sys
import time

lock_path = Path(sys.argv[1])
ready_path = Path(sys.argv[2])
handle = open(lock_path, "r+b")

if os.name == "nt":
    import msvcrt

    handle.seek(0)
    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)

    def unlock():
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
else:
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def unlock():
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

try:
    ready_path.write_text("locked", encoding="utf-8")
    time.sleep(0.75)
finally:
    unlock()
    handle.close()
"""


def _run_subprocess_migration_writers(
    tmp_path: Path,
    database_urls: list[str],
) -> list[tuple[str, str]]:
    start_path = tmp_path / "subprocess-start"
    sentinel_path = tmp_path / "subprocess-critical-section"
    ready_paths = [
        tmp_path / f"subprocess-ready-{index}"
        for index in range(len(database_urls))
    ]
    processes = [
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                _SUBPROCESS_MIGRATION_SCRIPT,
                database_url,
                str(start_path),
                str(sentinel_path),
                str(ready_path),
            ],
            cwd=Path(__file__).resolve().parents[1],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for database_url, ready_path in zip(database_urls, ready_paths)
    ]
    outputs: list[tuple[str, str]] = []
    try:
        deadline = time.monotonic() + 15
        while not all(path.exists() for path in ready_paths):
            if any(process.poll() is not None for process in processes):
                outputs = [
                    process.communicate(timeout=5)
                    if process.poll() is not None
                    else ("", "still running")
                    for process in processes
                ]
                raise AssertionError(
                    f"subprocess migration writer exited before barrier: {outputs}"
                )
            if time.monotonic() >= deadline:
                raise AssertionError("subprocess migration writers did not become ready")
            time.sleep(0.02)
        start_path.write_text("start", encoding="utf-8")
        outputs = [process.communicate(timeout=20) for process in processes]
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=5)

    assert [process.returncode for process in processes] == [
        0 for _ in processes
    ], outputs
    assert not sentinel_path.exists()
    return outputs


def test_check_missing_database_is_read_only(tmp_path: Path) -> None:
    database_path = tmp_path / "missing.db"

    state = check_migration_state(_sqlite_url(database_path))

    assert not database_path.exists()
    assert state.is_compatible is True
    assert state.is_current is False
    assert state.applied_versions == ()
    assert state.pending_versions == tuple(
        migration.version for migration in MIGRATIONS
    )
    assert state.error is None


def test_sqlite_path_normalization_supports_missing_file_and_memory(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "missing-parent" / "missing.db"

    assert _sqlite_database_path("sqlite:///:memory:") is None
    assert _sqlite_database_path(_sqlite_url(database_path)) == database_path.resolve(
        strict=False
    )


def test_apply_is_idempotent_and_reaches_current_version(tmp_path: Path) -> None:
    database_path = tmp_path / "fresh.db"
    database_url = _sqlite_url(database_path)

    first = apply_migrations(database_url)
    with sqlite3.connect(database_path) as connection:
        first_rows = connection.execute(
            "SELECT version, description, applied_at FROM schema_migrations"
        ).fetchall()
        first_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()

    second = apply_migrations(database_url)
    with sqlite3.connect(database_path) as connection:
        second_rows = connection.execute(
            "SELECT version, description, applied_at FROM schema_migrations"
        ).fetchall()
        second_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()

    assert first.is_current is True
    assert second.is_current is True
    assert first_rows == second_rows
    assert first_schema == second_schema
    assert [row[0] for row in second_rows] == [
        migration.version for migration in MIGRATIONS
    ]


def test_existing_baseline_is_converged_by_ordered_pr0_migration(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "production-baseline.db"
    database_url = _sqlite_url(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "CREATE TABLE schema_migrations ("
            "version VARCHAR(64) PRIMARY KEY, description VARCHAR(255) NOT NULL, "
            "applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        )
        connection.execute(
            "INSERT INTO schema_migrations (version, description) VALUES (?, ?)",
            (BASELINE_SCHEMA_VERSION, "existing production baseline"),
        )

    state = apply_migrations(database_url)

    assert state.is_current is True
    assert state.applied_versions == (
        BASELINE_SCHEMA_VERSION,
        PR0_CONVERGENCE_SCHEMA_VERSION,
        PR1_DURABLE_JOBS_SCHEMA_VERSION,
        PR2_RESEARCH_DATA_SCHEMA_VERSION,
        PR3_RESEARCH_EVIDENCE_SCHEMA_VERSION,
        PR4_RESEARCH_DEBATE_SCHEMA_VERSION,
        PERSONAL_RESEARCH_POLICY_SCHEMA_VERSION,
        PERSONAL_RESEARCH_SKILLS_SCHEMA_VERSION,
        DECISION_OUTCOME_V2_SCHEMA_VERSION,
        PERSONAL_RESEARCH_POLICY_CONTEXT_SCHEMA_VERSION,
    )
    with sqlite3.connect(database_path) as connection:
        llm_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(llm_usage)")
        }
        decision_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(decision_signals)")
        }
        analysis_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(analysis_history)")
        }
        unique_index_columns = {
            row[1]: tuple(
                info[2]
                for info in connection.execute(
                    f"PRAGMA index_info('{row[1]}')"
                )
            )
            for row in connection.execute("PRAGMA index_list(intelligence_items)")
            if row[2]
        }

    assert "provider_usage_json" in llm_columns
    assert _PR1_LLM_USAGE_AUDIT_COLUMNS.issubset(llm_columns)
    assert _PR1_LLM_USAGE_FORBIDDEN_GENERIC_COLUMNS.isdisjoint(llm_columns)
    assert "decision_profile" in decision_columns
    assert "idempotency_key" in decision_columns
    assert "job_id" in analysis_columns
    assert (
        "source_id",
        "url",
        "scope_type",
        "scope_value",
        "market",
    ) in unique_index_columns.values()


def test_fresh_pr1_schema_has_durable_tables_and_partial_unique_indexes(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "fresh-pr1.db"

    state = apply_migrations(_sqlite_url(database_path))

    assert state.is_current is True
    with sqlite3.connect(database_path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        assert {
            "analysis_jobs",
            "job_events",
            "notification_outbox",
            "provider_health",
        }.issubset(tables)

        llm_columns = {
            row[1] for row in connection.execute("PRAGMA table_info('llm_usage')")
        }
        llm_indexes = {
            row[1]: tuple(
                info[2]
                for info in connection.execute(f"PRAGMA index_info('{row[1]}')")
            )
            for row in connection.execute("PRAGMA index_list('llm_usage')")
            if row[1] in _PR1_LLM_USAGE_AUDIT_INDEXES
        }
        assert _PR1_LLM_USAGE_AUDIT_COLUMNS.issubset(llm_columns)
        assert _PR1_LLM_USAGE_FORBIDDEN_GENERIC_COLUMNS.isdisjoint(llm_columns)
        assert _llm_usage_audit_column_contract(connection) == {
            name: (column_type, False)
            for name, column_type in _PR1_LLM_USAGE_AUDIT_COLUMN_TYPES.items()
        }
        assert llm_indexes == _PR1_LLM_USAGE_AUDIT_INDEXES

        partial_unique_indexes = {
            row[1]: (bool(row[2]), bool(row[4]))
            for table_name in (
                "analysis_jobs",
                "analysis_history",
                "decision_signals",
            )
            for row in connection.execute(f"PRAGMA index_list('{table_name}')")
        }
        assert partial_unique_indexes["uix_analysis_jobs_idempotency_key"] == (
            True,
            True,
        )
        assert partial_unique_indexes["uix_analysis_jobs_active_dedupe_key"] == (
            True,
            True,
        )
        assert partial_unique_indexes[
            "uix_analysis_history_job_code_report_type"
        ] == (True, True)
        assert partial_unique_indexes[
            "uix_decision_signals_idempotency_key"
        ] == (True, True)


def test_pr1_migrates_pr0_shaped_schema_preserves_rows_and_is_idempotent(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "pr0-shaped.db"
    database_url = _sqlite_url(database_path)
    _create_pr0_shaped_schema(database_path)

    before = check_migration_state(database_url)
    first = apply_migrations(
        database_url,
        migrations=_migrations_through(PR4_RESEARCH_DEBATE_SCHEMA_VERSION),
    )
    with sqlite3.connect(database_path) as connection:
        first_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        first_rows = (
            connection.execute("SELECT COUNT(*) FROM llm_usage").fetchone()[0],
            connection.execute(
                "SELECT COUNT(*) FROM analysis_history"
            ).fetchone()[0],
            connection.execute(
                "SELECT COUNT(*) FROM decision_signals"
            ).fetchone()[0],
        )
        llm_columns = {
            row[1] for row in connection.execute("PRAGMA table_info('llm_usage')")
        }
        llm_indexes = {
            row[1]: tuple(
                info[2]
                for info in connection.execute(f"PRAGMA index_info('{row[1]}')")
            )
            for row in connection.execute("PRAGMA index_list('llm_usage')")
            if row[1] in _PR1_LLM_USAGE_AUDIT_INDEXES
        }
        audit_values = connection.execute(
            "SELECT latency_ms, error_code, error_message_sanitized, "
            "estimated_cost_usd, cost_source, attempt_no FROM llm_usage"
        ).fetchone()
        llm_audit_contract = _llm_usage_audit_column_contract(connection)
        history_columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info('analysis_history')")
        }
        signal_columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info('decision_signals')")
        }

    second = apply_migrations(
        database_url,
        migrations=_migrations_through(PR4_RESEARCH_DEBATE_SCHEMA_VERSION),
    )
    with sqlite3.connect(database_path) as connection:
        second_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        second_rows = (
            connection.execute("SELECT COUNT(*) FROM llm_usage").fetchone()[0],
            connection.execute(
                "SELECT COUNT(*) FROM analysis_history"
            ).fetchone()[0],
            connection.execute(
                "SELECT COUNT(*) FROM decision_signals"
            ).fetchone()[0],
        )

    assert before.pending_versions == (
        PR1_DURABLE_JOBS_SCHEMA_VERSION,
        PR2_RESEARCH_DATA_SCHEMA_VERSION,
        PR3_RESEARCH_EVIDENCE_SCHEMA_VERSION,
        PR4_RESEARCH_DEBATE_SCHEMA_VERSION,
        PERSONAL_RESEARCH_POLICY_SCHEMA_VERSION,
        PERSONAL_RESEARCH_SKILLS_SCHEMA_VERSION,
        DECISION_OUTCOME_V2_SCHEMA_VERSION,
        PERSONAL_RESEARCH_POLICY_CONTEXT_SCHEMA_VERSION,
    )
    assert first.is_current is True
    assert second.is_current is True
    assert first_schema == second_schema
    assert first_rows == second_rows == (1, 1, 1)
    assert _PR1_LLM_USAGE_AUDIT_COLUMNS.issubset(llm_columns)
    assert _PR1_LLM_USAGE_FORBIDDEN_GENERIC_COLUMNS.isdisjoint(llm_columns)
    assert llm_audit_contract == {
        name: (column_type, False)
        for name, column_type in _PR1_LLM_USAGE_AUDIT_COLUMN_TYPES.items()
    }
    assert llm_indexes == _PR1_LLM_USAGE_AUDIT_INDEXES
    assert audit_values == (None, None, None, None, None, None)
    assert "job_id" in history_columns
    assert "idempotency_key" in signal_columns


def test_pr1_rejects_ambiguous_generic_llm_audit_columns(tmp_path: Path) -> None:
    database_path = tmp_path / "ambiguous-llm-audit.db"
    database_url = _sqlite_url(database_path)
    _create_pr0_shaped_schema(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute("ALTER TABLE llm_usage ADD COLUMN error TEXT")

    with pytest.raises(RuntimeError, match="ambiguous audit columns: error"):
        apply_migrations(
            database_url,
            migrations=_migrations_through(PR4_RESEARCH_DEBATE_SCHEMA_VERSION),
        )

    state = check_migration_state(database_url)
    assert state.pending_versions == (
        PR1_DURABLE_JOBS_SCHEMA_VERSION,
        PR2_RESEARCH_DATA_SCHEMA_VERSION,
        PR3_RESEARCH_EVIDENCE_SCHEMA_VERSION,
        PR4_RESEARCH_DEBATE_SCHEMA_VERSION,
        PERSONAL_RESEARCH_POLICY_SCHEMA_VERSION,
        PERSONAL_RESEARCH_SKILLS_SCHEMA_VERSION,
        DECISION_OUTCOME_V2_SCHEMA_VERSION,
        PERSONAL_RESEARCH_POLICY_CONTEXT_SCHEMA_VERSION,
    )
    with sqlite3.connect(database_path) as connection:
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info('llm_usage')")
        }
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    assert columns == {"id", "called_at", "error"}
    assert "analysis_jobs" not in tables


def test_pr1_schema_failure_rolls_back_ddl_and_does_not_record_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import storage

    database_path = tmp_path / "pr1-rollback.db"
    database_url = _sqlite_url(database_path)
    _create_pr0_shaped_schema(
        database_path,
        include_migration_history=False,
    )
    with sqlite3.connect(database_path) as connection:
        before_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()

    def fail_contract(_connection) -> None:
        raise RuntimeError("injected PR1 schema verification failure")

    monkeypatch.setattr(
        storage,
        "_verify_pr1_durable_schema_contract",
        fail_contract,
    )
    with pytest.raises(RuntimeError, match="injected PR1"):
        apply_migrations(
            database_url,
            migrations=(
                Migration(
                    PR1_DURABLE_JOBS_SCHEMA_VERSION,
                    "injected rollback probe",
                    storage.run_pr1_durable_jobs_schema_upgrade,
                ),
            ),
        )

    with sqlite3.connect(database_path) as connection:
        after_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' "
            "AND name NOT IN ('schema_migrations', 'ix_schema_migrations_applied_at') "
            "ORDER BY type, name"
        ).fetchall()
        migration_rows = connection.execute(
            "SELECT version FROM schema_migrations"
        ).fetchall()

    assert after_schema == before_schema
    assert migration_rows == []


def _create_pr1_shaped_schema(database_path: Path) -> str:
    database_url = _sqlite_url(database_path)
    _create_pr0_shaped_schema(database_path)
    state = apply_migrations(
        database_url,
        migrations=_migrations_through(PR1_DURABLE_JOBS_SCHEMA_VERSION),
    )
    assert state.current_version == PR1_DURABLE_JOBS_SCHEMA_VERSION
    return database_url


def test_pr2_research_schema_contract_and_second_apply_are_stable(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "pr1-to-pr2.db"
    database_url = _create_pr1_shaped_schema(database_path)

    first = apply_migrations(
        database_url,
        migrations=_migrations_through(PR2_RESEARCH_DATA_SCHEMA_VERSION),
    )
    with sqlite3.connect(database_path) as connection:
        first_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        dataset_columns = {
            row[1]: (row[2], bool(row[3]), row[4])
            for row in connection.execute(
                "PRAGMA table_info('research_dataset_snapshots')"
            )
        }
        factor_columns = {
            row[1]: (row[2], bool(row[3]), row[4])
            for row in connection.execute(
                "PRAGMA table_info('research_factor_snapshots')"
            )
        }
        research_columns = {
            row[1]: (row[2], bool(row[3]), row[4])
            for row in connection.execute(
                "PRAGMA table_info('research_snapshots')"
            )
        }
        index_contract = {
            table_name: {
                row[1]: (
                    bool(row[2]),
                    tuple(
                        item[2]
                        for item in connection.execute(
                            f"PRAGMA index_info('{row[1]}')"
                        )
                    ),
                )
                for row in connection.execute(f"PRAGMA index_list('{table_name}')")
                if not row[1].startswith("sqlite_autoindex_")
            }
            for table_name in (
                "research_dataset_snapshots",
                "research_factor_snapshots",
                "research_snapshots",
            )
        }
        foreign_keys = {
            table_name: {
                (row[2], row[3], row[4], row[6].upper())
                for row in connection.execute(
                    f"PRAGMA foreign_key_list('{table_name}')"
                )
            }
            for table_name in (
                "research_dataset_snapshots",
                "research_factor_snapshots",
                "research_snapshots",
            )
        }

    second = apply_migrations(
        database_url,
        migrations=_migrations_through(PR2_RESEARCH_DATA_SCHEMA_VERSION),
    )
    with sqlite3.connect(database_path) as connection:
        second_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()

    assert first.is_current is True
    assert second.is_current is True
    assert first.applied_versions[-1] == PR2_RESEARCH_DATA_SCHEMA_VERSION
    assert first_schema == second_schema
    assert {
        "research_dataset_snapshots",
        "research_factor_snapshots",
        "research_snapshots",
    }.issubset(tables)
    assert dataset_columns["normalized_json"] == ("TEXT", False, None)
    assert dataset_columns["content_hash"] == ("CHAR(64)", True, None)
    assert factor_columns["primary_horizon"] == ("INTEGER", True, "10")
    assert factor_columns["coverage"] == ("FLOAT", True, None)
    assert factor_columns["content_hash"] == ("CHAR(64)", True, None)
    assert research_columns["canonical_json"] == ("TEXT", True, None)
    assert research_columns["snapshot_hash"] == ("CHAR(64)", True, None)
    assert research_columns["evidence_snapshot_hash"] == (
        "VARCHAR(64)",
        False,
        None,
    )
    assert research_columns["debate_snapshot_hash"] == (
        "VARCHAR(64)",
        False,
        None,
    )
    assert index_contract["research_dataset_snapshots"] == {
        "ix_research_dataset_snapshots_dataset_scope_asof": (
            False,
            ("dataset", "scope_type", "scope_value", "data_as_of", "id"),
        ),
        "ix_research_dataset_snapshots_scope_available": (
            False,
            ("scope_type", "scope_value", "available_at", "id"),
        ),
        "uix_research_dataset_snapshots_content_hash": (
            True,
            ("content_hash",),
        ),
    }
    assert index_contract["research_factor_snapshots"] == {
        "ix_research_factor_snapshots_stock_asof": (
            False,
            ("stock_code", "as_of"),
        ),
        "ix_research_factor_snapshots_stock_profile_asof": (
            False,
            ("stock_code", "company_profile", "as_of"),
        ),
        "uix_research_factor_snapshots_content_hash": (
            True,
            ("content_hash",),
        ),
    }
    assert index_contract["research_snapshots"] == {
        "ix_research_snapshots_debate_hash": (
            False,
            ("debate_snapshot_hash",),
        ),
        "ix_research_snapshots_evidence_hash": (
            False,
            ("evidence_snapshot_hash",),
        ),
        "ix_research_snapshots_stock_asof": (
            False,
            ("stock_code", "as_of"),
        ),
        "uix_research_snapshots_snapshot_hash": (
            True,
            ("snapshot_hash",),
        ),
    }
    expected_foreign_key = {("analysis_jobs", "origin_job_id", "task_id", "SET NULL")}
    assert all(value == expected_foreign_key for value in foreign_keys.values())


def test_pr2_schema_failure_rolls_back_all_ddl_and_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import storage

    database_path = tmp_path / "pr2-rollback.db"
    database_url = _create_pr1_shaped_schema(database_path)
    with sqlite3.connect(database_path) as connection:
        before_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()

    def fail_contract(_connection) -> None:
        raise RuntimeError("injected PR2 schema verification failure")

    monkeypatch.setattr(storage, "_verify_pr2_research_schema_contract", fail_contract)
    with pytest.raises(RuntimeError, match="injected PR2"):
        apply_migrations(
            database_url,
            migrations=_migrations_through(PR2_RESEARCH_DATA_SCHEMA_VERSION),
        )

    with sqlite3.connect(database_path) as connection:
        after_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        migration_versions = [
            row[0]
            for row in connection.execute(
                "SELECT version FROM schema_migrations ORDER BY version"
            )
        ]

    assert after_schema == before_schema
    assert PR2_RESEARCH_DATA_SCHEMA_VERSION not in migration_versions


def _create_historical_pr2_shaped_schema(database_path: Path) -> str:
    """Create the exact pre-PR3 shape, independent of current ORM metadata."""

    database_url = _create_pr1_shaped_schema(database_path)
    state = apply_migrations(
        database_url,
        migrations=_migrations_through(PR2_RESEARCH_DATA_SCHEMA_VERSION),
    )
    assert state.current_version == PR2_RESEARCH_DATA_SCHEMA_VERSION
    with sqlite3.connect(database_path) as connection:
        connection.execute("DROP INDEX ix_research_snapshots_debate_hash")
        connection.execute(
            "ALTER TABLE research_snapshots DROP COLUMN debate_snapshot_hash"
        )
        connection.execute("DROP INDEX ix_research_snapshots_evidence_hash")
        connection.execute(
            "ALTER TABLE research_snapshots DROP COLUMN evidence_snapshot_hash"
        )
        connection.execute(
            "INSERT INTO research_snapshots ("
            "stock_code, market, snapshot_version, field_dictionary_version, "
            "factor_engine_version, pack_version, prompt_version, policy_version, "
            "model_route_fingerprint, as_of, available_at, status, canonical_json, "
            "snapshot_hash, factor_snapshot_hash, origin_job_id"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "600519",
                "A",
                "research-v1",
                "fields-v1",
                "factor-v1",
                "pack-v1",
                "prompt-v1",
                "policy-v1",
                "route-v1",
                "2026-08-08 08:00:00",
                "2026-08-08 07:59:00",
                "available",
                "{}",
                "a" * 64,
                None,
                None,
            ),
        )
    return database_url


def test_pr3_evidence_schema_upgrades_historical_pr2_and_is_idempotent(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "pr2-to-pr3.db"
    database_url = _create_historical_pr2_shaped_schema(database_path)

    first = apply_migrations(
        database_url,
        migrations=_migrations_through(PR3_RESEARCH_EVIDENCE_SCHEMA_VERSION),
    )
    with sqlite3.connect(database_path) as connection:
        first_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        evidence_columns = {
            row[1]: (row[2], bool(row[3]), row[4])
            for row in connection.execute(
                "PRAGMA table_info('research_evidence_snapshots')"
            )
        }
        evidence_indexes = {
            row[1]: (
                bool(row[2]),
                tuple(
                    item[2]
                    for item in connection.execute(
                        f"PRAGMA index_info('{row[1]}')"
                    )
                ),
            )
            for row in connection.execute(
                "PRAGMA index_list('research_evidence_snapshots')"
            )
            if not row[1].startswith("sqlite_autoindex_")
        }
        research_columns = {
            row[1]: (row[2], bool(row[3]), row[4])
            for row in connection.execute(
                "PRAGMA table_info('research_snapshots')"
            )
        }
        research_indexes = {
            row[1]: tuple(
                item[2]
                for item in connection.execute(
                    f"PRAGMA index_info('{row[1]}')"
                )
            )
            for row in connection.execute(
                "PRAGMA index_list('research_snapshots')"
            )
        }
        evidence_foreign_keys = {
            (row[2], row[3], row[4], row[6].upper())
            for row in connection.execute(
                "PRAGMA foreign_key_list('research_evidence_snapshots')"
            )
        }
        preserved_research_row = connection.execute(
            "SELECT snapshot_hash, evidence_snapshot_hash "
            "FROM research_snapshots WHERE snapshot_hash = ?",
            ("a" * 64,),
        ).fetchone()

    second = apply_migrations(
        database_url,
        migrations=_migrations_through(PR3_RESEARCH_EVIDENCE_SCHEMA_VERSION),
    )
    with sqlite3.connect(database_path) as connection:
        second_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()

    assert first.current_version == PR3_RESEARCH_EVIDENCE_SCHEMA_VERSION
    assert second.is_current is True
    assert first_schema == second_schema
    assert evidence_columns == {
        "id": ("INTEGER", True, None),
        "stock_code": ("VARCHAR(16)", True, None),
        "market": ("VARCHAR(16)", True, None),
        "evidence_engine_version": ("VARCHAR(64)", True, None),
        "claim_policy_version": ("VARCHAR(64)", True, None),
        "as_of": ("DATETIME", True, None),
        "available_at": ("DATETIME", True, None),
        "status": ("VARCHAR(32)", True, None),
        "coverage": ("FLOAT", True, None),
        "claim_count": ("INTEGER", True, None),
        "citation_count": ("INTEGER", True, None),
        "canonical_json": ("TEXT", True, None),
        "input_dataset_hashes_json": ("TEXT", True, None),
        "factor_snapshot_hash": ("VARCHAR(64)", True, None),
        "evidence_hash": ("CHAR(64)", True, None),
        "origin_job_id": ("VARCHAR(64)", False, None),
        "created_at": ("DATETIME", True, "CURRENT_TIMESTAMP"),
    }
    assert evidence_indexes == {
        "ix_research_evidence_snapshots_factor_hash": (
            False,
            ("factor_snapshot_hash",),
        ),
        "ix_research_evidence_snapshots_stock_asof": (
            False,
            ("stock_code", "as_of"),
        ),
        "uix_research_evidence_snapshots_evidence_hash": (
            True,
            ("evidence_hash",),
        ),
    }
    assert research_columns["evidence_snapshot_hash"] == (
        "VARCHAR(64)",
        False,
        None,
    )
    assert research_indexes["ix_research_snapshots_evidence_hash"] == (
        "evidence_snapshot_hash",
    )
    assert evidence_foreign_keys == {
        ("analysis_jobs", "origin_job_id", "task_id", "SET NULL")
    }
    assert preserved_research_row == ("a" * 64, None)


def test_pr3_strict_verifier_rolls_back_wrong_index_and_version(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "pr3-wrong-index.db"
    database_url = _create_historical_pr2_shaped_schema(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "CREATE INDEX ix_research_snapshots_evidence_hash "
            "ON research_snapshots (snapshot_hash)"
        )

    with pytest.raises(RuntimeError, match="index .* is incompatible"):
        apply_migrations(
            database_url,
            migrations=_migrations_through(PR3_RESEARCH_EVIDENCE_SCHEMA_VERSION),
        )

    with sqlite3.connect(database_path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        columns = {
            row[1]
            for row in connection.execute(
                "PRAGMA table_info('research_snapshots')"
            )
        }
        versions = {
            row[0]
            for row in connection.execute("SELECT version FROM schema_migrations")
        }
    assert "research_evidence_snapshots" not in tables
    assert "evidence_snapshot_hash" not in columns
    assert PR3_RESEARCH_EVIDENCE_SCHEMA_VERSION not in versions


def test_pr3_schema_failure_rolls_back_all_ddl_and_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import storage

    database_path = tmp_path / "pr3-rollback.db"
    database_url = _create_historical_pr2_shaped_schema(database_path)
    with sqlite3.connect(database_path) as connection:
        before_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()

    def fail_contract(_connection) -> None:
        raise RuntimeError("injected PR3 schema verification failure")

    monkeypatch.setattr(
        storage,
        "_verify_pr3_research_evidence_schema_contract",
        fail_contract,
    )
    with pytest.raises(RuntimeError, match="injected PR3"):
        apply_migrations(
            database_url,
            migrations=_migrations_through(PR3_RESEARCH_EVIDENCE_SCHEMA_VERSION),
        )

    with sqlite3.connect(database_path) as connection:
        after_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        versions = {
            row[0]
            for row in connection.execute("SELECT version FROM schema_migrations")
        }

    assert after_schema == before_schema
    assert PR3_RESEARCH_EVIDENCE_SCHEMA_VERSION not in versions


def _create_historical_pr3_shaped_schema(database_path: Path) -> str:
    """Create the exact pre-PR4 shape without current debate ORM extensions."""

    database_url = _create_historical_pr2_shaped_schema(database_path)
    state = apply_migrations(
        database_url,
        migrations=_migrations_through(PR3_RESEARCH_EVIDENCE_SCHEMA_VERSION),
    )
    assert state.current_version == PR3_RESEARCH_EVIDENCE_SCHEMA_VERSION
    return database_url


def test_pr4_debate_schema_upgrades_historical_pr3_and_is_idempotent(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "pr3-to-pr4.db"
    database_url = _create_historical_pr3_shaped_schema(database_path)

    pr4_migrations = _migrations_through(PR4_RESEARCH_DEBATE_SCHEMA_VERSION)
    first = apply_migrations(database_url, migrations=pr4_migrations)
    with sqlite3.connect(database_path) as connection:
        first_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        columns = {
            table_name: {
                row[1]: (row[2], bool(row[3]), row[4])
                for row in connection.execute(
                    f"PRAGMA table_info('{table_name}')"
                )
            }
            for table_name in (
                "research_debate_requests",
                "research_debate_turns",
                "research_debate_snapshots",
            )
        }
        indexes = {
            table_name: {
                row[1]: (
                    bool(row[2]),
                    tuple(
                        item[2]
                        for item in connection.execute(
                            f"PRAGMA index_info('{row[1]}')"
                        )
                    ),
                )
                for row in connection.execute(
                    f"PRAGMA index_list('{table_name}')"
                )
                if not row[1].startswith("sqlite_autoindex_")
            }
            for table_name in (
                "research_debate_requests",
                "research_debate_turns",
                "research_debate_snapshots",
            )
        }
        foreign_keys = {
            table_name: {
                (row[2], row[3], row[4], row[6].upper())
                for row in connection.execute(
                    f"PRAGMA foreign_key_list('{table_name}')"
                )
            }
            for table_name in (
                "research_debate_requests",
                "research_debate_turns",
                "research_debate_snapshots",
            )
        }
        research_columns = {
            row[1]: (row[2], bool(row[3]), row[4])
            for row in connection.execute(
                "PRAGMA table_info('research_snapshots')"
            )
        }
        research_indexes = {
            row[1]: tuple(
                item[2]
                for item in connection.execute(
                    f"PRAGMA index_info('{row[1]}')"
                )
            )
            for row in connection.execute(
                "PRAGMA index_list('research_snapshots')"
            )
        }
        preserved = connection.execute(
            "SELECT snapshot_hash, evidence_snapshot_hash, debate_snapshot_hash "
            "FROM research_snapshots WHERE snapshot_hash = ?",
            ("a" * 64,),
        ).fetchone()
        snapshot_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' "
            "AND name = 'research_debate_snapshots'"
        ).fetchone()[0]

    second = apply_migrations(database_url, migrations=pr4_migrations)
    with sqlite3.connect(database_path) as connection:
        second_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()

    assert first.current_version == PR4_RESEARCH_DEBATE_SCHEMA_VERSION
    assert second.is_current is True
    assert first_schema == second_schema
    assert {
        "research_debate_requests",
        "research_debate_turns",
        "research_debate_snapshots",
    }.issubset(tables)
    assert columns["research_debate_requests"]["request_hash"] == (
        "CHAR(64)",
        True,
        None,
    )
    assert columns["research_debate_turns"]["request_hash"] == (
        "VARCHAR(64)",
        True,
        None,
    )
    assert columns["research_debate_turns"]["round_no"] == (
        "INTEGER",
        True,
        "1",
    )
    assert columns["research_debate_snapshots"]["debate_hash"] == (
        "CHAR(64)",
        True,
        None,
    )
    assert indexes["research_debate_requests"] == {
        "ix_research_debate_requests_evidence_route": (
            False,
            (
                "evidence_snapshot_hash",
                "prompt_version",
                "model_route_fingerprint",
            ),
        ),
        "ix_research_debate_requests_stock_asof": (
            False,
            ("stock_code", "as_of", "id"),
        ),
        "uix_research_debate_requests_request_hash": (
            True,
            ("request_hash",),
        ),
    }
    assert indexes["research_debate_turns"] == {
        "ix_research_debate_turns_resume": (
            False,
            (
                "stock_code",
                "evidence_snapshot_hash",
                "request_hash",
                "stance",
                "prompt_version",
                "prompt_fingerprint",
                "model_route_fingerprint",
            ),
        ),
        "ix_research_debate_turns_stock_asof": (
            False,
            ("stock_code", "as_of", "id"),
        ),
        "uix_research_debate_turns_turn_hash": (True, ("turn_hash",)),
    }
    assert indexes["research_debate_snapshots"] == {
        "ix_research_debate_snapshots_evidence_asof": (
            False,
            ("evidence_snapshot_hash", "as_of", "id"),
        ),
        "ix_research_debate_snapshots_request_hash": (
            False,
            ("request_hash",),
        ),
        "ix_research_debate_snapshots_stock_asof": (
            False,
            ("stock_code", "as_of", "id"),
        ),
        "uix_research_debate_snapshots_debate_hash": (
            True,
            ("debate_hash",),
        ),
    }
    origin_fk = ("analysis_jobs", "origin_job_id", "task_id", "SET NULL")
    request_fk = (
        "research_debate_requests",
        "request_hash",
        "request_hash",
        "RESTRICT",
    )
    assert foreign_keys["research_debate_requests"] == {origin_fk}
    assert foreign_keys["research_debate_turns"] == {origin_fk, request_fk}
    assert foreign_keys["research_debate_snapshots"] == {origin_fk, request_fk}
    assert research_columns["debate_snapshot_hash"] == (
        "VARCHAR(64)",
        False,
        None,
    )
    assert research_indexes["ix_research_snapshots_debate_hash"] == (
        "debate_snapshot_hash",
    )
    assert preserved == ("a" * 64, None, None)
    assert "ck_research_debate_snapshots_turn_presence" in snapshot_sql


def test_pr4_strict_verifier_rolls_back_wrong_index_and_version(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "pr4-wrong-index.db"
    database_url = _create_historical_pr3_shaped_schema(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "CREATE INDEX ix_research_snapshots_debate_hash "
            "ON research_snapshots (snapshot_hash)"
        )

    with pytest.raises(RuntimeError, match="index .* is incompatible"):
        apply_migrations(
            database_url,
            migrations=_migrations_through(PR4_RESEARCH_DEBATE_SCHEMA_VERSION),
        )

    with sqlite3.connect(database_path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        columns = {
            row[1]
            for row in connection.execute(
                "PRAGMA table_info('research_snapshots')"
            )
        }
        versions = {
            row[0]
            for row in connection.execute("SELECT version FROM schema_migrations")
        }
    assert "research_debate_requests" not in tables
    assert "research_debate_turns" not in tables
    assert "research_debate_snapshots" not in tables
    assert "debate_snapshot_hash" not in columns
    assert PR4_RESEARCH_DEBATE_SCHEMA_VERSION not in versions


def test_pr4_schema_failure_rolls_back_all_ddl_and_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import storage

    database_path = tmp_path / "pr4-rollback.db"
    database_url = _create_historical_pr3_shaped_schema(database_path)
    with sqlite3.connect(database_path) as connection:
        before_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()

    def fail_contract(_connection) -> None:
        raise RuntimeError("injected PR4 schema verification failure")

    monkeypatch.setattr(
        storage,
        "_verify_pr4_research_debate_schema_contract",
        fail_contract,
    )
    with pytest.raises(RuntimeError, match="injected PR4"):
        apply_migrations(
            database_url,
            migrations=_migrations_through(PR4_RESEARCH_DEBATE_SCHEMA_VERSION),
        )

    with sqlite3.connect(database_path) as connection:
        after_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        versions = {
            row[0]
            for row in connection.execute("SELECT version FROM schema_migrations")
        }

    assert after_schema == before_schema
    assert PR4_RESEARCH_DEBATE_SCHEMA_VERSION not in versions


_PERSONAL_RESEARCH_TABLES = (
    "research_watchlist_items",
    "portfolio_reconciliations",
    "portfolio_reconciliation_adjustments",
    "portfolio_policy_evaluations",
    "research_budget_reservations",
)
_PERSONAL_RESEARCH_SKILL_TABLES = (
    "personal_research_skill_contracts",
    "personal_research_skill_executions",
    "personal_research_debate_reviews",
    "personal_research_theses",
)
_DECISION_OUTCOME_V2_TABLES = ("decision_outcomes_v2",)
_PERSONAL_RESEARCH_SIGNAL_COLUMNS = (
    "research_stance",
    "account_action",
    "value_quality_score",
    "trend_timing_score",
    "catalyst_score",
    "risk_score",
    "evidence_quality_score",
    "research_snapshot_hash",
    "policy_version",
    "policy_hash",
    "policy_evaluation_hash",
    "portfolio_snapshot_ref",
    "prompt_version",
    "catalysts_json",
    "invalidators_json",
    "unknowns_json",
    "evidence_refs_json",
    "policy_mode",
    "policy_decision",
    "would_block",
    "policy_reasons_json",
)
_PERSONAL_RESEARCH_SIGNAL_INDEXES = tuple(
    f"ix_decision_signals_{column}"
    for column in (
        "research_stance",
        "account_action",
        "research_snapshot_hash",
        "policy_version",
        "policy_hash",
        "policy_evaluation_hash",
        "portfolio_snapshot_ref",
        "policy_mode",
        "policy_decision",
        "would_block",
    )
)


def _create_historical_pr4_complete_schema(database_path: Path) -> str:
    """Create the full pre-personal-research schema including Portfolio tables."""

    database_url = _sqlite_url(database_path)
    state = apply_migrations(
        database_url,
        migrations=_migrations_through(PR4_RESEARCH_DEBATE_SCHEMA_VERSION),
    )
    assert state.current_version == PR4_RESEARCH_DEBATE_SCHEMA_VERSION

    # The baseline migration intentionally uses current ORM metadata.  Remove
    # the new append-only extension to reconstruct the exact historical PR4
    # boundary while retaining the complete legacy Portfolio schema.
    with sqlite3.connect(database_path) as connection:
        for table_name in reversed(_DECISION_OUTCOME_V2_TABLES):
            connection.execute(f'DROP TABLE "{table_name}"')
        for table_name in reversed(_PERSONAL_RESEARCH_SKILL_TABLES):
            connection.execute(f'DROP TABLE "{table_name}"')
        for table_name in reversed(_PERSONAL_RESEARCH_TABLES):
            connection.execute(f'DROP TABLE "{table_name}"')
        for index_name in _PERSONAL_RESEARCH_SIGNAL_INDEXES:
            connection.execute(f'DROP INDEX "{index_name}"')
        for column_name in reversed(_PERSONAL_RESEARCH_SIGNAL_COLUMNS):
            connection.execute(
                f'ALTER TABLE decision_signals DROP COLUMN "{column_name}"'
            )
        connection.execute(
            "INSERT INTO portfolio_accounts (id, owner_id, name, market, "
            "base_currency, is_active) VALUES (1, 'owner-1', 'primary', "
            "'cn', 'CNY', 1)"
        )
        connection.execute(
            "INSERT INTO decision_signals ("
            "id, stock_code, market, source_type, trigger_source, action, "
            "plan_quality, status"
            ") VALUES (1, '600519', 'cn', 'report', 'manual', 'observe', "
            "'unknown', 'active')"
        )
    return database_url


def test_personal_research_policy_migration_preserves_pr4_and_is_idempotent(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "pr4-to-personal-research.db"
    database_url = _create_historical_pr4_complete_schema(database_path)

    first = apply_migrations(
        database_url,
        migrations=_migrations_through(PERSONAL_RESEARCH_POLICY_SCHEMA_VERSION),
    )
    with sqlite3.connect(database_path) as connection:
        first_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        signal_columns = {
            row[1]: (row[2], bool(row[3]), row[4])
            for row in connection.execute("PRAGMA table_info('decision_signals')")
        }
        opening_index = next(
            row
            for row in connection.execute(
                "PRAGMA index_list('portfolio_reconciliations')"
            )
            if row[1] == "uix_portfolio_reconciliation_applied_opening"
        )
        preserved_account = connection.execute(
            "SELECT owner_id, name, market FROM portfolio_accounts WHERE id = 1"
        ).fetchone()
        preserved_signal = connection.execute(
            "SELECT stock_code, action, would_block "
            "FROM decision_signals WHERE id = 1"
        ).fetchone()

        common_reconciliation = (
            "INSERT INTO portfolio_reconciliations ("
            "account_id, event_type, status, event_version, effective_date, "
            "preview_token, input_hash, request_json, diff_json, expires_at, "
            "applied_at) VALUES (?, 'opening', 'applied', ?, '2026-08-10', "
            "?, ?, '{}', '{}', '2026-08-11', '2026-08-10')"
        )
        connection.execute(
            common_reconciliation,
            (1, 1, "a" * 64, "b" * 64),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                common_reconciliation,
                (1, 2, "c" * 64, "d" * 64),
            )
        connection.execute(
            "INSERT INTO portfolio_accounts (id, owner_id, name, market, "
            "base_currency, is_active) VALUES (2, 'owner-2', 'secondary', "
            "'cn', 'CNY', 1)"
        )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="applied reconciliation adjustments are closed",
        ):
            connection.execute(
                "INSERT INTO portfolio_reconciliation_adjustments ("
                "reconciliation_id, account_id, identity_key, adjustment_type, "
                "stock_code, market, currency, quantity_delta, total_cost_delta, "
                "cash_delta, before_json, after_json"
                ") VALUES (1, 1, 'position:cn:600519', 'position', '600519', "
                "'cn', 'CNY', 1, 100, 0, '{}', '{}')"
            )
        connection.execute(
            "INSERT INTO portfolio_reconciliations ("
            "account_id, event_type, status, effective_date, preview_token, "
            "input_hash, request_json, diff_json, expires_at"
            ") VALUES (1, 'adjustment', 'preview', '2026-08-10', ?, ?, '{}', "
            "'{}', '2026-08-11')",
            ("e" * 64, "f" * 64),
        )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="reconciliation adjustment account mismatch",
        ):
            connection.execute(
                "INSERT INTO portfolio_reconciliation_adjustments ("
                "reconciliation_id, account_id, identity_key, adjustment_type, "
                "stock_code, market, currency, quantity_delta, total_cost_delta, "
                "cash_delta, before_json, after_json"
                ") VALUES (2, 2, 'position:cn:600519', 'position', '600519', "
                "'cn', 'CNY', 1, 100, 0, '{}', '{}')"
            )
        connection.execute(
            "INSERT INTO portfolio_reconciliation_adjustments ("
            "reconciliation_id, account_id, identity_key, adjustment_type, "
            "stock_code, market, currency, quantity_delta, total_cost_delta, "
            "cash_delta, before_json, after_json"
            ") VALUES (2, 1, 'position:cn:600519', 'position', '600519', "
            "'cn', 'CNY', 1, 100, 0, '{}', '{}')"
        )
        connection.execute(
            "UPDATE portfolio_reconciliations SET status = 'applied', "
            "event_version = 2, idempotency_key = 'adjustment-2', "
            "applied_at = '2026-08-10' WHERE id = 2"
        )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="applied reconciliation is immutable",
        ):
            connection.execute(
                "UPDATE portfolio_reconciliations SET account_id = 2 WHERE id = 1"
            )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="applied reconciliation is immutable",
        ):
            connection.execute(
                "UPDATE portfolio_reconciliations SET note = 'tampered' WHERE id = 1"
            )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="reconciliation adjustment is immutable",
        ):
            connection.execute(
                "UPDATE portfolio_reconciliation_adjustments "
                "SET quantity_delta = 2 WHERE reconciliation_id = 2"
            )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="reconciliation adjustment is immutable",
        ):
            connection.execute(
                "DELETE FROM portfolio_reconciliation_adjustments "
                "WHERE reconciliation_id = 2"
            )
        with pytest.raises(
            sqlite3.IntegrityError,
            match="applied reconciliation is immutable",
        ):
            connection.execute(
                "DELETE FROM portfolio_reconciliations WHERE id = 1"
            )

    second = apply_migrations(
        database_url,
        migrations=_migrations_through(PERSONAL_RESEARCH_POLICY_SCHEMA_VERSION),
    )
    with sqlite3.connect(database_path) as connection:
        second_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()

    assert first.is_current is True
    assert second.is_current is True
    assert first.current_version == PERSONAL_RESEARCH_POLICY_SCHEMA_VERSION
    assert first_schema == second_schema
    assert set(_PERSONAL_RESEARCH_TABLES).issubset(tables)
    assert set(_PERSONAL_RESEARCH_SIGNAL_COLUMNS).issubset(signal_columns)
    assert signal_columns["would_block"] == ("BOOLEAN", True, "0")
    assert bool(opening_index[2]) is True
    assert bool(opening_index[4]) is True
    assert preserved_account == ("owner-1", "primary", "cn")
    assert preserved_signal == ("600519", "observe", 0)


def test_personal_research_policy_schema_failure_rolls_back_all_ddl_and_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import storage

    database_path = tmp_path / "personal-research-rollback.db"
    database_url = _create_historical_pr4_complete_schema(database_path)
    with sqlite3.connect(database_path) as connection:
        before_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()

    def fail_contract(_connection) -> None:
        raise RuntimeError("injected personal research schema verification failure")

    monkeypatch.setattr(
        storage,
        "_verify_personal_research_policy_schema_contract",
        fail_contract,
    )
    with pytest.raises(RuntimeError, match="injected personal research"):
        apply_migrations(database_url)

    with sqlite3.connect(database_path) as connection:
        after_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        versions = {
            row[0]
            for row in connection.execute("SELECT version FROM schema_migrations")
        }

    assert after_schema == before_schema
    assert PERSONAL_RESEARCH_POLICY_SCHEMA_VERSION not in versions


def _create_historical_personal_research_policy_schema(database_path: Path) -> str:
    """Create the exact schema boundary immediately before PR4 artifacts."""

    database_url = _create_historical_pr4_complete_schema(database_path)
    state = apply_migrations(
        database_url,
        migrations=_migrations_through(PERSONAL_RESEARCH_POLICY_SCHEMA_VERSION),
    )
    assert state.current_version == PERSONAL_RESEARCH_POLICY_SCHEMA_VERSION
    return database_url


def test_personal_research_skills_migration_is_seeded_immutable_and_idempotent(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "personal-research-skills.db"
    database_url = _create_historical_personal_research_policy_schema(database_path)

    first = apply_migrations(
        database_url,
        migrations=_migrations_through(PERSONAL_RESEARCH_SKILLS_SCHEMA_VERSION),
    )
    with sqlite3.connect(database_path) as connection:
        first_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        contracts = connection.execute(
            "SELECT skill_id, skill_version, contract_hash, score_field "
            "FROM personal_research_skill_contracts ORDER BY rowid"
        ).fetchall()
        trigger_names = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'trigger' "
                "AND name LIKE 'trg_personal_research_%'"
            )
        }
        preserved_signal = connection.execute(
            "SELECT stock_code, market FROM decision_signals WHERE id = 1"
        ).fetchone()
        with pytest.raises(
            sqlite3.IntegrityError,
            match="skill contract is immutable",
        ):
            connection.execute(
                "UPDATE personal_research_skill_contracts "
                "SET score_field = 'risk_score' "
                "WHERE skill_id = 'personal-value-quality'"
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO personal_research_skill_contracts "
                "(skill_id, skill_version, contract_hash, score_field, canonical_json) "
                "VALUES ('personal-fake', '1.0.0', ?, 'risk_score', '{}')",
                ("f" * 64,),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO personal_research_skill_contracts "
                "(skill_id, skill_version, contract_hash, score_field, canonical_json) "
                "VALUES ('personal-value-quality', '1.0.0', ?, "
                "'value_quality_score', '{}')",
                ("e" * 64,),
            )

    second = apply_migrations(
        database_url,
        migrations=_migrations_through(PERSONAL_RESEARCH_SKILLS_SCHEMA_VERSION),
    )
    with sqlite3.connect(database_path) as connection:
        second_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()

    assert first.is_current is True
    assert second.is_current is True
    assert first.current_version == PERSONAL_RESEARCH_SKILLS_SCHEMA_VERSION
    assert first_schema == second_schema
    assert set(_PERSONAL_RESEARCH_SKILL_TABLES).issubset(tables)
    assert len(contracts) == 5
    assert {row[0] for row in contracts} == {
        "personal-value-quality",
        "personal-trend-timing",
        "personal-catalyst",
        "personal-risk",
        "personal-evidence-quality",
    }
    assert all(row[1] == "1.0.0" and len(row[2]) == 64 for row in contracts)
    assert {
        "trg_personal_research_skill_execution_update",
        "trg_personal_research_skill_execution_delete",
        "trg_personal_research_debate_review_update",
        "trg_personal_research_debate_review_delete",
        "trg_personal_research_thesis_update",
        "trg_personal_research_thesis_delete",
    }.issubset(trigger_names)
    assert preserved_signal == ("600519", "cn")


def test_personal_research_skills_schema_failure_rolls_back_all_ddl_and_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import storage

    database_path = tmp_path / "personal-research-skills-rollback.db"
    database_url = _create_historical_personal_research_policy_schema(database_path)
    with sqlite3.connect(database_path) as connection:
        before_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()

    def fail_contract(_connection) -> None:
        raise RuntimeError("injected personal research Skills verification failure")

    monkeypatch.setattr(
        storage,
        "_verify_personal_research_skills_schema_contract",
        fail_contract,
    )
    with pytest.raises(RuntimeError, match="injected personal research Skills"):
        apply_migrations(
            database_url,
            migrations=_migrations_through(PERSONAL_RESEARCH_SKILLS_SCHEMA_VERSION),
        )

    with sqlite3.connect(database_path) as connection:
        after_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        versions = {
            row[0]
            for row in connection.execute("SELECT version FROM schema_migrations")
        }

    assert after_schema == before_schema
    assert PERSONAL_RESEARCH_SKILLS_SCHEMA_VERSION not in versions


def _create_historical_personal_research_skills_schema(
    database_path: Path,
) -> str:
    """Create the exact schema boundary before Decision Outcome v2."""

    database_url = _create_historical_personal_research_policy_schema(database_path)
    state = apply_migrations(
        database_url,
        migrations=_migrations_through(PERSONAL_RESEARCH_SKILLS_SCHEMA_VERSION),
    )
    assert state.current_version == PERSONAL_RESEARCH_SKILLS_SCHEMA_VERSION
    return database_url


def test_decision_outcome_v2_migration_is_strict_idempotent_and_preserves_v1(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "decision-outcome-v2.db"
    database_url = _create_historical_personal_research_skills_schema(
        database_path
    )
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "INSERT INTO decision_signal_outcomes ("
            "signal_id, horizon, engine_version, eval_status, outcome, "
            "direction_expected, direction_correct, stock_return_pct, "
            "action, market, holding_state"
            ") VALUES (1, '5d', 'decision-signal-v1', 'completed', 'hit', "
            "'up', 1, 5.0, 'observe', 'cn', 'unknown')"
        )

    first = apply_migrations(database_url)
    with sqlite3.connect(database_path) as connection:
        first_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        table_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' "
            "AND name = 'decision_outcomes_v2'"
        ).fetchone()[0]
        foreign_keys = connection.execute(
            "PRAGMA foreign_key_list('decision_outcomes_v2')"
        ).fetchall()
        indexes = {
            row[1]: (bool(row[2]), bool(row[4]))
            for row in connection.execute(
                "PRAGMA index_list('decision_outcomes_v2')"
            )
        }
        trigger_names = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'trigger' "
                "AND name LIKE 'trg_decision_outcome_v2_%'"
            )
        }
        preserved_v1 = connection.execute(
            "SELECT signal_id, horizon, engine_version, eval_status, outcome "
            "FROM decision_signal_outcomes"
        ).fetchall()

    second = apply_migrations(database_url)
    with sqlite3.connect(database_path) as connection:
        second_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()

    assert first.is_current is True
    assert second.is_current is True
    assert first.current_version == PERSONAL_RESEARCH_POLICY_CONTEXT_SCHEMA_VERSION
    assert first_schema == second_schema
    assert "decision-outcome-v2" in table_sql
    assert "'pending', 'evaluated', 'observational'" in table_sql
    assert "'unexecutable', 'unable'" in table_sql
    assert any(
        row[2] == "decision_signals"
        and row[3] == "signal_id"
        and row[4] == "id"
        and row[6].upper() == "RESTRICT"
        for row in foreign_keys
    )
    assert indexes["ix_decision_outcome_v2_candidates"] == (False, False)
    assert indexes["ix_decision_outcome_v2_calibration"] == (False, False)
    assert {
        "trg_decision_outcome_v2_lineage_insert",
        "trg_decision_outcome_v2_dataset_insert",
        "trg_decision_outcome_v2_terminal_update",
        "trg_decision_outcome_v2_frozen_update",
        "trg_decision_outcome_v2_lineage_update",
        "trg_decision_outcome_v2_dataset_update",
        "trg_decision_outcome_v2_delete",
        "trg_decision_outcome_v2_signal_delete_restrict",
        "trg_decision_outcome_v2_dataset_snapshot_update_restrict",
        "trg_decision_outcome_v2_dataset_snapshot_delete_restrict",
    } == trigger_names
    assert preserved_v1 == [
        (1, "5d", "decision-signal-v1", "completed", "hit")
    ]


def test_decision_outcome_v2_schema_failure_rolls_back_ddl_and_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import storage

    database_path = tmp_path / "decision-outcome-v2-rollback.db"
    database_url = _create_historical_personal_research_skills_schema(
        database_path
    )
    with sqlite3.connect(database_path) as connection:
        before_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()

    def fail_contract(_connection) -> None:
        raise RuntimeError("injected Decision Outcome v2 verification failure")

    monkeypatch.setattr(
        storage,
        "_verify_decision_outcome_v2_schema_contract",
        fail_contract,
    )
    with pytest.raises(RuntimeError, match="injected Decision Outcome v2"):
        apply_migrations(database_url)

    with sqlite3.connect(database_path) as connection:
        after_schema = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        versions = {
            row[0]
            for row in connection.execute("SELECT version FROM schema_migrations")
        }

    assert after_schema == before_schema
    assert DECISION_OUTCOME_V2_SCHEMA_VERSION not in versions


def test_apply_serializes_concurrent_writers(tmp_path: Path) -> None:
    database_path = tmp_path / "concurrent.db"
    database_url = _sqlite_url(database_path)
    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def apply_once(engine) -> None:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                "CREATE TABLE migration_probe (id INTEGER PRIMARY KEY, value TEXT NOT NULL)"
            )
            connection.exec_driver_sql(
                "INSERT INTO migration_probe (id, value) VALUES (1, 'once')"
            )

    migrations = (Migration("001-probe", "single writer probe", apply_once),)

    def worker() -> None:
        try:
            barrier.wait(timeout=5)
            apply_migrations(database_url, migrations=migrations)
        except BaseException as exc:  # pragma: no cover - assertion reports details
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert not any(thread.is_alive() for thread in threads)
    assert errors == []
    with sqlite3.connect(database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM migration_probe").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0] == 1


def test_apply_serializes_subprocess_writers(tmp_path: Path) -> None:
    database_path = tmp_path / "subprocess-concurrent.db"
    database_url = _sqlite_url(database_path)

    _run_subprocess_migration_writers(tmp_path, [database_url, database_url])

    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM subprocess_probe"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM schema_migrations"
        ).fetchone()[0] == 1


def test_zero_byte_lock_file_waits_for_existing_process_owner(
    tmp_path: Path,
) -> None:
    """A crashed empty lock file must not make a concurrent startup fail."""

    database_path = tmp_path / "zero-byte-lock.db"
    database_url = _sqlite_url(database_path)
    lock_path = Path(f"{database_path.resolve(strict=False)}.migration.lock")
    lock_path.touch()
    assert lock_path.stat().st_size == 0

    ready_path = tmp_path / "empty-lock-holder-ready"
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            _SUBPROCESS_EMPTY_LOCK_HOLDER,
            str(lock_path),
            str(ready_path),
        ],
        cwd=Path(__file__).resolve().parents[1],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    holder_output: tuple[str, str] = ("", "")
    try:
        deadline = time.monotonic() + 10
        while not ready_path.exists():
            if holder.poll() is not None:
                holder_output = holder.communicate(timeout=5)
                raise AssertionError(
                    f"empty lock holder exited before acquiring lock: {holder_output}"
                )
            if time.monotonic() >= deadline:
                raise AssertionError("empty lock holder did not become ready")
            time.sleep(0.02)

        def apply_once(engine) -> None:
            with engine.begin() as connection:
                connection.exec_driver_sql(
                    "CREATE TABLE zero_byte_lock_probe (id INTEGER PRIMARY KEY)"
                )

        state = apply_migrations(
            database_url,
            migrations=(
                Migration("001-zero-byte-lock", "zero-byte lock recovery", apply_once),
            ),
            lock_timeout_seconds=5,
        )
        holder_output = holder.communicate(timeout=5)
    finally:
        if holder.poll() is None:
            holder.kill()
            holder_output = holder.communicate(timeout=5)

    assert holder.returncode == 0, holder_output
    assert state.is_current is True
    assert lock_path.read_bytes() == b"0"
    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM zero_byte_lock_probe"
        ).fetchone()[0] == 0


def test_symlink_database_aliases_share_subprocess_writer_lock(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "canonical.db"
    alias_path = tmp_path / "database-alias.db"
    connection = sqlite3.connect(database_path)
    connection.close()
    try:
        alias_path.symlink_to(database_path)
    except (NotImplementedError, OSError) as exc:
        pytest.skip(f"file symlinks are unavailable on this platform: {exc}")

    canonical_url = _sqlite_url(database_path)
    alias_url = _sqlite_url(alias_path)
    assert _sqlite_database_path(alias_url) == database_path.resolve(strict=False)

    _run_subprocess_migration_writers(tmp_path, [canonical_url, alias_url])

    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM subprocess_probe"
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM schema_migrations"
        ).fetchone()[0] == 1


def test_unknown_database_version_is_incompatible(tmp_path: Path) -> None:
    database_path = tmp_path / "ahead.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "CREATE TABLE schema_migrations ("
            "version TEXT PRIMARY KEY, description TEXT NOT NULL, applied_at DATETIME NOT NULL)"
        )
        connection.execute(
            "INSERT INTO schema_migrations VALUES ('2099-01-01-future', 'future', CURRENT_TIMESTAMP)"
        )
    database_url = _sqlite_url(database_path)

    state = check_migration_state(database_url)

    assert state.is_compatible is False
    assert state.is_current is False
    assert state.unknown_versions == ("2099-01-01-future",)
    with pytest.raises(MigrationError, match="unknown to this build"):
        apply_migrations(database_url)


@pytest.mark.parametrize(
    "legacy_unique_sql",
    [
        "CREATE UNIQUE INDEX uix_intelligence_item_url_legacy "
        "ON intelligence_items(url)",
        "CREATE UNIQUE INDEX uix_intel_item_scope ON intelligence_items("
        "source_id, url, scope_type, scope_value, market)",
    ],
    ids=("url-unique", "old-columns-only-repair"),
)
def test_legacy_intelligence_rebuild_preserves_constraints_and_indexes(
    tmp_path: Path,
    legacy_unique_sql: str,
) -> None:
    database_path = tmp_path / "legacy-intelligence.db"
    database_url = _sqlite_url(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.executescript(
            f"""
            CREATE TABLE schema_migrations (
                version VARCHAR(64) PRIMARY KEY,
                description VARCHAR(255) NOT NULL,
                applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            INSERT INTO schema_migrations (version, description)
            VALUES ('2026-06-05-create-all-baseline', 'legacy baseline');

            CREATE TABLE intelligence_sources (
                id INTEGER PRIMARY KEY
            );
            INSERT INTO intelligence_sources (id) VALUES (7);

            CREATE TABLE intelligence_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id INTEGER,
                source_name VARCHAR(100),
                source_type VARCHAR(32) NOT NULL DEFAULT 'rss',
                title VARCHAR(300) NOT NULL,
                summary TEXT,
                url VARCHAR(1000) NOT NULL,
                source VARCHAR(100),
                published_at DATETIME,
                fetched_at DATETIME,
                scope_type VARCHAR(32) NOT NULL DEFAULT 'market',
                scope_value VARCHAR(64),
                market VARCHAR(32) NOT NULL DEFAULT 'cn',
                raw_payload TEXT
            );
            {legacy_unique_sql};
            INSERT INTO intelligence_items (
                id, source_id, source_name, source_type, title, url,
                scope_type, scope_value, market
            ) VALUES (
                1, 7, 'legacy', 'rss', 'legacy item',
                'https://example.invalid/legacy', 'market', NULL, 'cn'
            );
            """
        )

    first = apply_migrations(database_url)
    assert first.is_current is True

    def inspect_contract() -> tuple[
        list[tuple],
        list[tuple],
        list[tuple],
        list[tuple],
    ]:
        with sqlite3.connect(database_path) as connection:
            schema = connection.execute(
                "SELECT type, name, sql FROM sqlite_master "
                "WHERE tbl_name = 'intelligence_items' "
                "ORDER BY type, name"
            ).fetchall()
            foreign_keys = connection.execute(
                "PRAGMA foreign_key_list(intelligence_items)"
            ).fetchall()
            indexes = []
            for index_row in connection.execute(
                "PRAGMA index_list(intelligence_items)"
            ).fetchall():
                index_name = index_row[1]
                columns = tuple(
                    row[2]
                    for row in connection.execute(
                        f"PRAGMA index_info('{index_name}')"
                    ).fetchall()
                )
                indexes.append((index_name, int(index_row[2]), columns))
            rows = connection.execute(
                "SELECT id, source_id, scope_value FROM intelligence_items ORDER BY id"
            ).fetchall()
        return schema, foreign_keys, sorted(indexes), rows

    first_schema, foreign_keys, indexes, rows = inspect_contract()
    index_map = {name: columns for name, _unique, columns in indexes}
    unique_shapes = {columns for _name, unique, columns in indexes if unique}

    assert any(
        row[2] == "intelligence_sources"
        and row[3] == "source_id"
        and row[4] == "id"
        and row[6].upper() == "SET NULL"
        for row in foreign_keys
    )
    assert index_map["ix_intel_item_scope_time"] == (
        "scope_type",
        "scope_value",
        "market",
        "published_at",
    )
    assert index_map["ix_intel_item_fetch_time"] == ("fetched_at",)
    assert (
        "source_id",
        "url",
        "scope_type",
        "scope_value",
        "market",
    ) in unique_shapes
    assert not any("recreate_tmp" in name for name in index_map)
    assert rows == [(1, 7, "__dsa_null_scope__")]

    second = apply_migrations(database_url)
    second_schema, second_foreign_keys, second_indexes, second_rows = inspect_contract()

    assert second.is_current is True
    assert second_schema == first_schema
    assert second_foreign_keys == foreign_keys
    assert second_indexes == indexes
    assert second_rows == rows


def test_cli_check_uses_distinct_pending_exit_code_without_writes(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_path = tmp_path / "cli-missing.db"

    exit_code = main([
        "--check",
        "--database-url",
        _sqlite_url(database_path),
    ])

    assert exit_code == 2
    assert not database_path.exists()
    assert '"is_current": false' in capsys.readouterr().out


def test_cli_apply_returns_json_for_non_migration_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_apply(*args, **kwargs):
        raise PermissionError("database is read-only")

    monkeypatch.setattr("src.migrations.apply_migrations", fail_apply)

    exit_code = main([
        "--apply",
        "--database-url",
        _sqlite_url(tmp_path / "permission.db"),
    ])

    assert exit_code == 1
    assert capsys.readouterr().out.strip() == (
        '{\n  "error": "database is read-only"\n}'
    )


def test_explicit_service_mode_rejects_pending_schema_without_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import storage

    database_path = tmp_path / "explicit-pending.db"
    database_url = _sqlite_url(database_path)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "CREATE TABLE schema_migrations ("
            "version VARCHAR(64) PRIMARY KEY, description VARCHAR(255) NOT NULL, "
            "applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        )
        connection.execute(
            "INSERT INTO schema_migrations (version, description) VALUES (?, ?)",
            (BASELINE_SCHEMA_VERSION, "existing production baseline"),
        )
        before_objects = connection.execute(
            "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
        ).fetchall()

    runtime_config = SimpleNamespace(
        database_migration_mode="explicit",
        sqlite_wal_enabled=True,
        sqlite_busy_timeout_ms=5000,
        sqlite_write_retry_max=3,
        sqlite_write_retry_base_delay=0.1,
    )
    monkeypatch.setattr(storage, "get_config", lambda: runtime_config)
    storage.DatabaseManager.reset_instance()
    try:
        with pytest.raises(storage.DatabaseMigrationRequired, match="run `python -m"):
            storage.DatabaseManager(db_url=database_url)
    finally:
        storage.DatabaseManager.reset_instance()

    with sqlite3.connect(database_path) as connection:
        after_objects = connection.execute(
            "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
        ).fetchall()
        versions = connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall()

    assert after_objects == before_objects
    assert versions == [(BASELINE_SCHEMA_VERSION,)]
