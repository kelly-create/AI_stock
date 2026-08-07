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
    MIGRATIONS,
    PR0_CONVERGENCE_SCHEMA_VERSION,
    Migration,
    MigrationError,
    _sqlite_database_path,
    apply_migrations,
    check_migration_state,
    main,
)


def _sqlite_url(path: Path) -> str:
    return f"sqlite:///{path.as_posix()}"


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
    )
    with sqlite3.connect(database_path) as connection:
        llm_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(llm_usage)")
        }
        decision_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(decision_signals)")
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
    assert "decision_profile" in decision_columns
    assert (
        "source_id",
        "url",
        "scope_type",
        "scope_value",
        "market",
    ) in unique_index_columns.values()


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
