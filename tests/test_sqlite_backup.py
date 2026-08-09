from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import closing
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from scripts import sqlite_backup
from src.storage import Base


CORE_TABLES = ("schema_migrations", "parents", "children", "items")


def test_default_backup_contract_includes_durable_and_research_tables() -> None:
    assert {
        "analysis_jobs",
        "job_events",
        "notification_outbox",
        "provider_health",
        "research_dataset_snapshots",
        "research_factor_snapshots",
        "research_evidence_snapshots",
        "research_debate_requests",
        "research_debate_turns",
        "research_debate_snapshots",
        "research_snapshots",
    }.issubset(sqlite_backup.DEFAULT_BACKUP_CORE_TABLES)


def test_default_backup_round_trip_preserves_evidence_rows(tmp_path: Path) -> None:
    database = tmp_path / "evidence-source.sqlite"
    backup = tmp_path / "evidence-backup.sqlite"
    restored = tmp_path / "evidence-restored.sqlite"
    engine = create_engine(f"sqlite:///{database.as_posix()}")
    try:
        Base.metadata.create_all(engine)
        with engine.begin() as connection:
            connection.exec_driver_sql(
                "INSERT INTO research_evidence_snapshots ("
                "stock_code, market, evidence_engine_version, claim_policy_version, "
                "as_of, available_at, status, coverage, claim_count, citation_count, "
                "canonical_json, input_dataset_hashes_json, factor_snapshot_hash, "
                "evidence_hash, origin_job_id"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "600519",
                    "A",
                    "evidence-v1",
                    "claim-policy-v1",
                    "2026-08-08 08:00:00",
                    "2026-08-08 07:59:00",
                    "available",
                    1.0,
                    0,
                    0,
                    "{}",
                    "[]",
                    "f" * 64,
                    "e" * 64,
                    None,
                ),
            )
            connection.exec_driver_sql(
                "INSERT INTO research_debate_requests ("
                "stock_code, market, debate_engine_version, output_schema_version, "
                "prompt_version, evidence_snapshot_hash, model_route_fingerprint, "
                "as_of, available_at, canonical_json, request_hash, origin_job_id"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "600519",
                    "A",
                    "research-debate-v1",
                    "research-debate-output-v1",
                    "research-debate-prompt-v1",
                    "e" * 64,
                    "a" * 64,
                    "2026-08-08 08:00:00",
                    "2026-08-08 07:59:00",
                    "{}",
                    "1" * 64,
                    None,
                ),
            )
            for stance, turn_hash, prompt_hash in (
                ("bull", "2" * 64, "4" * 64),
                ("bear", "3" * 64, "5" * 64),
            ):
                connection.exec_driver_sql(
                    "INSERT INTO research_debate_turns ("
                    "stock_code, market, stance, round_no, debate_engine_version, "
                    "output_schema_version, prompt_version, evidence_snapshot_hash, "
                    "request_hash, prompt_fingerprint, model_route_fingerprint, "
                    "model_used, as_of, available_at, canonical_json, turn_hash, "
                    "origin_job_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        "600519",
                        "A",
                        stance,
                        1,
                        "research-debate-v1",
                        "research-debate-output-v1",
                        "research-debate-prompt-v1",
                        "e" * 64,
                        "1" * 64,
                        prompt_hash,
                        "a" * 64,
                        "bounded-model-v1",
                        "2026-08-08 08:00:00",
                        "2026-08-08 07:59:00",
                        "{}",
                        turn_hash,
                        None,
                    ),
                )
            connection.exec_driver_sql(
                "INSERT INTO research_debate_snapshots ("
                "stock_code, market, debate_engine_version, output_schema_version, "
                "prompt_version, evidence_snapshot_hash, request_hash, "
                "model_route_fingerprint, as_of, available_at, status, "
                "bull_turn_hash, bear_turn_hash, bull_argument_count, "
                "bear_argument_count, open_question_count, canonical_json, "
                "debate_hash, origin_job_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "600519",
                    "A",
                    "research-debate-v1",
                    "research-debate-output-v1",
                    "research-debate-prompt-v1",
                    "e" * 64,
                    "1" * 64,
                    "a" * 64,
                    "2026-08-08 08:00:00",
                    "2026-08-08 07:59:00",
                    "available",
                    "2" * 64,
                    "3" * 64,
                    1,
                    1,
                    0,
                    "{}",
                    "6" * 64,
                    None,
                ),
            )
    finally:
        engine.dispose()

    manifest = sqlite_backup.create_backup(database, backup)
    result = sqlite_backup.restore_backup(backup, restored)

    assert manifest["database"]["core_table_counts"][
        "research_evidence_snapshots"
    ] == 1
    assert manifest["database"]["core_table_counts"][
        "research_debate_requests"
    ] == 1
    assert manifest["database"]["core_table_counts"][
        "research_debate_turns"
    ] == 2
    assert manifest["database"]["core_table_counts"][
        "research_debate_snapshots"
    ] == 1
    assert result["sha256"] == manifest["backup"]["sha256"]
    with closing(sqlite3.connect(restored)) as connection:
        row = connection.execute(
            "SELECT evidence_hash, factor_snapshot_hash "
            "FROM research_evidence_snapshots"
        ).fetchone()
        debate_row = connection.execute(
            "SELECT debate_hash, request_hash, bull_turn_hash, bear_turn_hash "
            "FROM research_debate_snapshots"
        ).fetchone()
    assert row == ("e" * 64, "f" * 64)
    assert debate_row == ("6" * 64, "1" * 64, "2" * 64, "3" * 64)


def _create_database(path: Path, *, item_prefix: str = "seed", item_count: int = 8) -> None:
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.executescript(
            """
            CREATE TABLE schema_migrations (
                version TEXT PRIMARY KEY,
                description TEXT,
                applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE parents (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
            CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL REFERENCES parents(id)
            );
            CREATE TABLE items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                value TEXT NOT NULL
            );
            CREATE INDEX ix_items_value ON items(value);
            """
        )
        connection.executemany(
            "INSERT INTO schema_migrations(version, description) VALUES (?, ?)",
            [("2026-01-01-baseline", "baseline"), ("2026-08-08-pr1", "durable jobs")],
        )
        connection.execute("INSERT INTO parents(id, name) VALUES (1, 'parent')")
        connection.execute("INSERT INTO children(id, parent_id) VALUES (1, 1)")
        connection.executemany(
            "INSERT INTO items(value) VALUES (?)",
            [(f"{item_prefix}-{index}",) for index in range(item_count)],
        )
        connection.commit()


def _schema_rows(path: Path) -> list[tuple[str, str, str]]:
    with closing(sqlite3.connect(path)) as connection:
        return connection.execute(
            """
            SELECT type, name, COALESCE(sql, '')
            FROM sqlite_master
            WHERE type IN ('table', 'index')
            ORDER BY type, name
            """
        ).fetchall()


def _rewrite_manifest_with_valid_canonical_hash(path: Path, payload: dict[str, object]) -> None:
    payload["manifest_sha256"] = sqlite_backup._manifest_canonical_sha256(payload)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_online_backup_with_active_wal_writer_restores_schema_indexes_and_counts(tmp_path: Path) -> None:
    database = tmp_path / "live.sqlite"
    backup = tmp_path / "live-backup.sqlite"
    restored = tmp_path / "isolated" / "restored.sqlite"
    restored.parent.mkdir()
    _create_database(database, item_count=50)
    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone()[0].lower() == "wal"

    started = threading.Event()
    stop = threading.Event()

    def write_while_backing_up() -> None:
        with closing(sqlite3.connect(database, timeout=10.0)) as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            index = 0
            while not stop.is_set():
                connection.execute("INSERT INTO items(value) VALUES (?)", (f"writer-{index}",))
                connection.commit()
                index += 1
                started.set()

    writer = threading.Thread(target=write_while_backing_up, daemon=True)
    writer.start()
    assert started.wait(timeout=5.0)
    try:
        manifest = sqlite_backup.create_backup(database, backup, core_tables=CORE_TABLES)
    finally:
        stop.set()
        writer.join(timeout=10.0)
    assert not writer.is_alive()

    verified = sqlite_backup.verify_backup(backup)
    result = sqlite_backup.restore_backup(backup, restored)

    assert verified == manifest
    assert result["status"] == "restored"
    assert result["rollback_backup"] is None
    assert manifest["database"]["quick_check"] == {"status": "ok", "issue_count": 0}
    assert manifest["database"]["foreign_key_check"] == {"status": "ok", "violation_count": 0}
    assert manifest["database"]["core_table_counts"]["items"] >= 51
    with closing(sqlite3.connect(backup)) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0].lower() == "delete"
    assert _schema_rows(restored) == _schema_rows(backup)
    with closing(sqlite3.connect(restored)) as connection:
        restored_counts = {
            table: connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            for table in CORE_TABLES
        }
    assert restored_counts == manifest["database"]["core_table_counts"]


def test_verify_rejects_bad_hash(tmp_path: Path) -> None:
    database = tmp_path / "source.sqlite"
    backup = tmp_path / "backup.sqlite"
    _create_database(database)
    sqlite_backup.create_backup(database, backup, core_tables=CORE_TABLES)
    manifest_path = sqlite_backup._default_manifest_path(backup)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["backup"]["sha256"] = "0" * 64
    _rewrite_manifest_with_valid_canonical_hash(manifest_path, manifest)

    with pytest.raises(sqlite_backup.BackupError, match="hash does not match"):
        sqlite_backup.verify_backup(backup)


def test_verify_rejects_manifest_metadata_tampering(tmp_path: Path) -> None:
    database = tmp_path / "source.sqlite"
    backup = tmp_path / "backup.sqlite"
    _create_database(database)
    sqlite_backup.create_backup(database, backup, core_tables=CORE_TABLES)
    manifest_path = sqlite_backup._default_manifest_path(backup)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["created_at_utc"] = "2000-01-01T00:00:00Z"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(sqlite_backup.BackupError, match="manifest canonical hash"):
        sqlite_backup.verify_backup(backup)


def test_verify_rejects_truncated_backup(tmp_path: Path) -> None:
    database = tmp_path / "source.sqlite"
    backup = tmp_path / "backup.sqlite"
    _create_database(database)
    sqlite_backup.create_backup(database, backup, core_tables=CORE_TABLES)
    original_size = backup.stat().st_size
    with backup.open("r+b") as handle:
        handle.truncate(original_size // 2)

    with pytest.raises(sqlite_backup.BackupError, match="size does not match"):
        sqlite_backup.verify_backup(backup)


def test_verify_rejects_incomplete_backup_manifest_pair(tmp_path: Path) -> None:
    database = tmp_path / "source.sqlite"
    backup = tmp_path / "backup.sqlite"
    _create_database(database)
    sqlite_backup.create_backup(database, backup, core_tables=CORE_TABLES)
    manifest_path = sqlite_backup._default_manifest_path(backup)
    manifest_bytes = manifest_path.read_bytes()

    manifest_path.unlink()
    with pytest.raises(sqlite_backup.BackupError, match="manifest must be a regular file"):
        sqlite_backup.verify_backup(backup)

    manifest_path.write_bytes(manifest_bytes)
    backup.unlink()
    with pytest.raises(sqlite_backup.BackupError, match="backup must be a regular file"):
        sqlite_backup.verify_backup(backup)


def test_restore_refuses_existing_isolated_target_without_changing_it(tmp_path: Path) -> None:
    database = tmp_path / "source.sqlite"
    backup = tmp_path / "backup.sqlite"
    target = tmp_path / "existing.sqlite"
    _create_database(database)
    sqlite_backup.create_backup(database, backup, core_tables=CORE_TABLES)
    target.write_bytes(b"do-not-overwrite")

    with pytest.raises(sqlite_backup.BackupError, match="already exists"):
        sqlite_backup.restore_backup(backup, target)

    assert target.read_bytes() == b"do-not-overwrite"


def test_isolated_restore_removes_target_if_directory_fsync_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "source.sqlite"
    backup = tmp_path / "backup.sqlite"
    target = tmp_path / "isolated.sqlite"
    _create_database(database)
    sqlite_backup.create_backup(database, backup, core_tables=CORE_TABLES)

    def fail_directory_fsync(_directory: Path) -> None:
        raise OSError("simulated directory fsync failure")

    monkeypatch.setattr(sqlite_backup, "_fsync_directory", fail_directory_fsync)

    with pytest.raises(sqlite_backup.BackupError, match="restore publication failed"):
        sqlite_backup.restore_backup(backup, target)

    assert not target.exists()


def test_failed_manifest_publication_removes_half_published_backup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "source.sqlite"
    backup = tmp_path / "backup.sqlite"
    manifest = sqlite_backup._default_manifest_path(backup)
    _create_database(database)
    real_atomic_publish_new = sqlite_backup._atomic_publish_new
    calls = 0

    def fail_second_publication(source: Path, destination: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated manifest publication failure")
        real_atomic_publish_new(source, destination)

    monkeypatch.setattr(sqlite_backup, "_atomic_publish_new", fail_second_publication)

    with pytest.raises(sqlite_backup.BackupError, match="publication failed"):
        sqlite_backup.create_backup(database, backup, core_tables=CORE_TABLES)

    assert not backup.exists()
    assert not manifest.exists()
    assert not list(tmp_path.glob(".*.tmp"))


def test_backup_does_not_overwrite_path_created_after_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "source.sqlite"
    backup = tmp_path / "backup.sqlite"
    victim = b"created-after-preflight"
    _create_database(database)
    real_atomic_publish_new = sqlite_backup._atomic_publish_new

    def plant_destination_before_publish(source: Path, destination: Path) -> None:
        if destination == backup:
            destination.write_bytes(victim)
        real_atomic_publish_new(source, destination)

    monkeypatch.setattr(sqlite_backup, "_atomic_publish_new", plant_destination_before_publish)

    with pytest.raises(sqlite_backup.BackupError, match="publication failed"):
        sqlite_backup.create_backup(database, backup, core_tables=CORE_TABLES)

    assert backup.read_bytes() == victim
    assert not sqlite_backup._default_manifest_path(backup).exists()


def test_isolated_restore_does_not_overwrite_path_created_after_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "source.sqlite"
    backup = tmp_path / "backup.sqlite"
    target = tmp_path / "restored.sqlite"
    victim = b"created-after-preflight"
    _create_database(database)
    sqlite_backup.create_backup(database, backup, core_tables=CORE_TABLES)
    real_atomic_publish_new = sqlite_backup._atomic_publish_new

    def plant_destination_before_publish(source: Path, destination: Path) -> None:
        if destination == target:
            destination.write_bytes(victim)
        real_atomic_publish_new(source, destination)

    monkeypatch.setattr(sqlite_backup, "_atomic_publish_new", plant_destination_before_publish)

    with pytest.raises(sqlite_backup.BackupError, match="publication failed"):
        sqlite_backup.restore_backup(backup, target)

    assert target.read_bytes() == victim


def test_integrity_failure_is_not_published(tmp_path: Path) -> None:
    database = tmp_path / "invalid.sqlite"
    backup = tmp_path / "backup.sqlite"
    _create_database(database)
    with closing(sqlite3.connect(database)) as connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("INSERT INTO children(id, parent_id) VALUES (2, 999)")
        connection.commit()

    with pytest.raises(sqlite_backup.BackupError, match="integrity or required-table checks failed"):
        sqlite_backup.create_backup(database, backup, core_tables=CORE_TABLES)

    assert not backup.exists()
    assert not sqlite_backup._default_manifest_path(backup).exists()


def test_production_replace_requires_declaration_and_preserves_old_database(tmp_path: Path) -> None:
    database = tmp_path / "source.sqlite"
    backup = tmp_path / "backup.sqlite"
    production = tmp_path / "production.sqlite"
    _create_database(database, item_prefix="new", item_count=6)
    _create_database(production, item_prefix="old", item_count=3)
    sqlite_backup.create_backup(database, backup, core_tables=CORE_TABLES)

    with pytest.raises(sqlite_backup.BackupError, match="services-stopped"):
        sqlite_backup.restore_backup(backup, production, replace_production=True)

    result = sqlite_backup.restore_backup(
        backup,
        production,
        replace_production=True,
        services_stopped=True,
    )
    rollback = tmp_path / result["rollback_backup"]
    rollback_manifest = tmp_path / result["rollback_manifest"]

    assert rollback.is_file()
    assert rollback_manifest.is_file()
    sqlite_backup.verify_backup(rollback, rollback_manifest)
    with closing(sqlite3.connect(production)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 6
        assert connection.execute("SELECT value FROM items ORDER BY id LIMIT 1").fetchone()[0] == "new-0"
    with closing(sqlite3.connect(rollback)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 3
        assert connection.execute("SELECT value FROM items ORDER BY id LIMIT 1").fetchone()[0] == "old-0"


def test_production_replace_fsync_failure_rolls_back_and_fsyncs_old_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "source.sqlite"
    backup = tmp_path / "backup.sqlite"
    production = tmp_path / "production.sqlite"
    _create_database(database, item_prefix="new", item_count=6)
    _create_database(production, item_prefix="old", item_count=3)
    sqlite_backup.create_backup(database, backup, core_tables=CORE_TABLES)
    fsync_calls = 0

    def fail_replacement_fsync_once(_directory: Path) -> None:
        nonlocal fsync_calls
        fsync_calls += 1
        # The rollback backup and manifest consume the first two calls. Fail
        # publication of the new production target, then allow rollback fsync.
        if fsync_calls == 3:
            raise OSError("simulated production directory fsync failure")

    monkeypatch.setattr(sqlite_backup, "_fsync_directory", fail_replacement_fsync_once)

    with pytest.raises(sqlite_backup.BackupError, match="restore publication failed"):
        sqlite_backup.restore_backup(
            backup,
            production,
            replace_production=True,
            services_stopped=True,
        )

    assert fsync_calls == 4
    with closing(sqlite3.connect(production)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 3
        assert connection.execute("SELECT value FROM items ORDER BY id LIMIT 1").fetchone()[0] == "old-0"


def test_restore_rejects_target_sidecars(tmp_path: Path) -> None:
    database = tmp_path / "source.sqlite"
    backup = tmp_path / "backup.sqlite"
    target = tmp_path / "isolated.sqlite"
    _create_database(database)
    sqlite_backup.create_backup(database, backup, core_tables=CORE_TABLES)
    target.with_name(f"{target.name}-wal").write_bytes(b"stale")

    with pytest.raises(sqlite_backup.BackupError, match="sidecars"):
        sqlite_backup.restore_backup(backup, target)

    assert not target.exists()


@pytest.mark.parametrize("suffix", sqlite_backup.SQLITE_SIDECAR_SUFFIXES)
def test_verify_and_restore_reject_backup_sidecars(tmp_path: Path, suffix: str) -> None:
    database = tmp_path / "source.sqlite"
    backup = tmp_path / "backup.sqlite"
    target = tmp_path / "restored.sqlite"
    _create_database(database)
    sqlite_backup.create_backup(database, backup, core_tables=CORE_TABLES)
    backup.with_name(f"{backup.name}{suffix}").write_bytes(b"stale-or-uncommitted")

    with pytest.raises(sqlite_backup.BackupError, match="sidecars"):
        sqlite_backup.verify_backup(backup)
    with pytest.raises(sqlite_backup.BackupError, match="sidecars"):
        sqlite_backup.restore_backup(backup, target)

    assert not target.exists()


@pytest.mark.parametrize("candidate_kind", ("backup", "manifest"))
@pytest.mark.parametrize("suffix", sqlite_backup.SQLITE_SIDECAR_SUFFIXES)
def test_backup_rejects_source_sidecar_path_alias(
    tmp_path: Path,
    candidate_kind: str,
    suffix: str,
) -> None:
    database = tmp_path / "source.sqlite"
    normal_backup = tmp_path / "backup.sqlite"
    alias = database.with_name(f"{database.name}{suffix}")
    _create_database(database)

    kwargs = {"manifest_path": alias} if candidate_kind == "manifest" else {}
    backup = normal_backup if candidate_kind == "manifest" else alias
    with pytest.raises(sqlite_backup.BackupError, match="SQLite path families"):
        sqlite_backup.create_backup(database, backup, core_tables=CORE_TABLES, **kwargs)

    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 8


@pytest.mark.parametrize("suffix", sqlite_backup.SQLITE_SIDECAR_SUFFIXES)
def test_backup_rejects_manifest_that_aliases_backup_sidecar(tmp_path: Path, suffix: str) -> None:
    database = tmp_path / "source.sqlite"
    backup = tmp_path / "backup.sqlite"
    manifest = backup.with_name(f"{backup.name}{suffix}")
    _create_database(database)

    with pytest.raises(sqlite_backup.BackupError, match="SQLite path families"):
        sqlite_backup.create_backup(
            database,
            backup,
            manifest_path=manifest,
            core_tables=CORE_TABLES,
        )

    assert not backup.exists()
    assert not manifest.exists()


@pytest.mark.parametrize("suffix", sqlite_backup.SQLITE_SIDECAR_SUFFIXES)
def test_restore_rejects_target_that_aliases_backup_sidecar(tmp_path: Path, suffix: str) -> None:
    database = tmp_path / "source.sqlite"
    backup = tmp_path / "backup.sqlite"
    target = backup.with_name(f"{backup.name}{suffix}")
    _create_database(database)
    sqlite_backup.create_backup(database, backup, core_tables=CORE_TABLES)

    with pytest.raises(sqlite_backup.BackupError, match="SQLite sidecars"):
        sqlite_backup.restore_backup(backup, target)

    assert not target.exists()
    sqlite_backup.verify_backup(backup)
