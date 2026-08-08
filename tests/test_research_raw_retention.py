from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
from unittest.mock import patch

import pytest

from src.services.research import raw_retention
from src.services.research.raw_retention import (
    RawSnapshotRecord,
    build_raw_retention_plan,
    execute_raw_retention,
    main,
)
from src.services.research.raw_store import RawArtifactStore
from src.services.research.worker_owner import TushareWorkerOwnerLock


NOW = datetime(2026, 8, 8, 8, 0, tzinfo=timezone.utc)


def _artifact(root: Path, value: object) -> tuple[dict[str, object], Path]:
    reference = RawArtifactStore(root).put_json(value).to_dict()
    return reference, root / str(reference["relative_path"])


def _record(
    dataset: str,
    reference: dict[str, object] | str,
    *,
    observed_at: datetime,
    created_at: datetime | None = None,
    record_id: int = 1,
    last_referenced_at: datetime | None = None,
) -> RawSnapshotRecord:
    return RawSnapshotRecord(
        record_id=record_id,
        dataset=dataset,
        raw_ref_json=reference if isinstance(reference, str) else json.dumps(reference),
        observed_at=observed_at,
        created_at=created_at or observed_at,
        last_referenced_at=last_referenced_at,
    )


def _create_cli_database(
    database_path: Path,
    entries: list[tuple[str, dict[str, object], datetime, object]],
) -> list[str]:
    content_hashes: list[str] = []
    with sqlite3.connect(database_path) as connection:
        connection.executescript(
            """
            CREATE TABLE research_dataset_snapshots (
                id INTEGER PRIMARY KEY,
                dataset TEXT NOT NULL,
                raw_ref_json TEXT,
                normalized_json TEXT,
                observed_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                content_hash TEXT NOT NULL
            );
            CREATE TABLE job_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )
        for index, (dataset, reference, observed_at, normalized) in enumerate(
            entries,
            start=1,
        ):
            content_hash = hashlib.sha256(f"snapshot-{index}".encode()).hexdigest()
            content_hashes.append(content_hash)
            connection.execute(
                """
                INSERT INTO research_dataset_snapshots
                    (id, dataset, raw_ref_json, normalized_json, observed_at,
                     created_at, content_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    index,
                    dataset,
                    json.dumps(reference),
                    json.dumps(normalized),
                    observed_at.isoformat(),
                    observed_at.isoformat(),
                    content_hash,
                ),
            )
    return content_hashes


def test_shared_content_hash_is_kept_when_any_reference_is_unexpired(tmp_path: Path) -> None:
    root = tmp_path / "research" / "raw"
    reference, json_path = _artifact(root, {"shared": True})
    bin_relative = str(reference["relative_path"]).replace(".json.gz", ".bin.gz")
    bin_path = root / bin_relative
    shutil.copyfile(json_path, bin_path)

    expired_reference = dict(reference, relative_path=bin_relative)
    records = (
        _record("daily", expired_reference, observed_at=NOW - timedelta(days=31), record_id=1),
        _record("daily", reference, observed_at=NOW - timedelta(days=30), record_id=2),
    )

    plan = build_raw_retention_plan(records, root, as_of=NOW)

    assert plan.blocked is False
    assert plan.expired_references == 1
    assert plan.protected_references == 1
    assert plan.candidate_paths == ()
    assert json_path.exists()
    assert bin_path.exists()


def test_shared_content_hash_is_kept_by_permanent_financial_reference(tmp_path: Path) -> None:
    root = tmp_path / "research" / "raw"
    reference, json_path = _artifact(root, {"shared-financial": True})
    bin_relative = str(reference["relative_path"]).replace(".json.gz", ".bin.gz")
    bin_path = root / bin_relative
    shutil.copyfile(json_path, bin_path)

    records = (
        _record(
            "daily",
            dict(reference, relative_path=bin_relative),
            observed_at=NOW - timedelta(days=31),
            record_id=1,
        ),
        _record("income", reference, observed_at=NOW - timedelta(days=3000), record_id=2),
    )

    plan = build_raw_retention_plan(records, root, as_of=NOW)

    assert plan.candidate_paths == ()
    assert plan.unknown_datasets == ()
    assert json_path.exists()
    assert bin_path.exists()


def test_exact_retention_boundary_is_kept_and_older_value_expires(tmp_path: Path) -> None:
    root = tmp_path / "research" / "raw"
    boundary_ref, boundary_path = _artifact(root, {"boundary": True})
    expired_ref, expired_path = _artifact(root, {"expired": True})

    records = (
        _record("daily", boundary_ref, observed_at=NOW - timedelta(days=30), record_id=1),
        _record(
            "daily",
            expired_ref,
            observed_at=NOW - timedelta(days=30, microseconds=1),
            record_id=2,
        ),
    )

    plan = build_raw_retention_plan(records, root, as_of=NOW)

    assert plan.candidate_paths == (str(expired_ref["relative_path"]),)
    assert boundary_path.exists()
    assert expired_path.exists()


def test_created_at_extends_window_beyond_old_observation(tmp_path: Path) -> None:
    root = tmp_path / "research" / "raw"
    reference, artifact_path = _artifact(root, {"created-later": True})
    record = _record(
        "daily",
        reference,
        observed_at=NOW - timedelta(days=365),
        created_at=NOW - timedelta(days=1),
    )

    plan = build_raw_retention_plan((record,), root, as_of=NOW)

    assert plan.candidate_paths == ()
    assert artifact_path.exists()


def test_dry_run_is_default_and_apply_removes_only_expired_file(tmp_path: Path) -> None:
    root = tmp_path / "research" / "raw"
    reference, artifact_path = _artifact(root, {"delete": True})
    record = _record("daily", reference, observed_at=NOW - timedelta(days=31))

    dry_run = execute_raw_retention((record,), root, as_of=NOW)

    assert dry_run.mode == "dry-run"
    assert dry_run.deleted_paths == ()
    assert dry_run.plan.candidate_paths == (str(reference["relative_path"]),)
    assert artifact_path.exists()

    applied = execute_raw_retention((record,), root, as_of=NOW, apply=True)

    assert applied.blocked is False
    assert applied.deleted_paths == (str(reference["relative_path"]),)
    assert applied.reclaimed_bytes > 0
    assert not artifact_path.exists()


def test_unreferenced_raw_is_collected_only_after_orphan_grace_period(
    tmp_path: Path,
) -> None:
    root = tmp_path / "research" / "raw"
    old_reference, old_path = _artifact(root, {"orphan": "old"})
    _fresh_reference, fresh_path = _artifact(root, {"orphan": "fresh"})
    old_timestamp = (NOW - timedelta(days=2)).timestamp()
    fresh_timestamp = (NOW - timedelta(hours=12)).timestamp()
    os.utime(old_path, (old_timestamp, old_timestamp))
    os.utime(fresh_path, (fresh_timestamp, fresh_timestamp))

    plan = build_raw_retention_plan((), root, as_of=NOW)

    assert plan.blocked is False
    assert plan.orphan_paths == (str(old_reference["relative_path"]),)
    assert plan.candidate_paths == plan.orphan_paths

    applied = execute_raw_retention((), root, as_of=NOW, apply=True)

    assert applied.deleted_paths == plan.orphan_paths
    assert not old_path.exists()
    assert fresh_path.exists()


def test_corrupt_old_orphan_blocks_retention_before_any_move(tmp_path: Path) -> None:
    root = tmp_path / "research" / "raw"
    expired_reference, expired_path = _artifact(root, {"referenced": "expired"})
    _orphan_reference, orphan_path = _artifact(root, {"orphan": "corrupt"})
    orphan_path.write_bytes(b"not-gzip")
    old_timestamp = (NOW - timedelta(days=2)).timestamp()
    os.utime(orphan_path, (old_timestamp, old_timestamp))
    record = _record(
        "daily",
        expired_reference,
        observed_at=NOW - timedelta(days=31),
    )

    result = execute_raw_retention((record,), root, as_of=NOW, apply=True)

    assert result.blocked is True
    assert result.deleted_paths == ()
    assert any("orphan" in error for error in result.plan.blocking_errors)
    assert expired_path.exists()
    assert orphan_path.exists()


def test_search_is_90_days_and_permanent_or_unknown_datasets_never_expire(tmp_path: Path) -> None:
    root = tmp_path / "research" / "raw"
    search_old, _ = _artifact(root, {"search": "old"})
    search_boundary, _ = _artifact(root, {"search": "boundary"})
    financial, _ = _artifact(root, {"financial": True})
    event, _ = _artifact(root, {"event": True})
    unknown, unknown_path = _artifact(root, {"unknown": True})

    records = (
        _record("news", search_old, observed_at=NOW - timedelta(days=91), record_id=1),
        _record("search", search_boundary, observed_at=NOW - timedelta(days=90), record_id=2),
        _record("income", financial, observed_at=NOW - timedelta(days=3000), record_id=3),
        _record("events", event, observed_at=NOW - timedelta(days=3000), record_id=4),
        _record("future_dataset_typo", unknown, observed_at=NOW - timedelta(days=3000), record_id=5),
    )

    plan = build_raw_retention_plan(records, root, as_of=NOW)

    assert plan.candidate_paths == (str(search_old["relative_path"]),)
    assert plan.unknown_datasets == ("future_dataset_typo",)
    assert unknown_path.exists()


def test_path_traversal_blocks_apply_and_never_touches_outside_file(tmp_path: Path) -> None:
    root = tmp_path / "research" / "raw"
    safe_ref, safe_path = _artifact(root, {"safe-candidate": True})
    outside = tmp_path / "outside.json.gz"
    outside.write_bytes(b"must remain")
    traversal_ref = dict(safe_ref, relative_path="../../outside.json.gz")
    records = (
        _record("daily", safe_ref, observed_at=NOW - timedelta(days=31), record_id=1),
        _record("daily", traversal_ref, observed_at=NOW - timedelta(days=31), record_id=2),
    )

    result = execute_raw_retention(records, root, as_of=NOW, apply=True)

    assert result.blocked is True
    assert result.deleted_paths == ()
    assert any("not safely relative" in error for error in result.plan.blocking_errors)
    assert safe_path.exists()
    assert outside.read_bytes() == b"must remain"


def test_symlinked_hash_prefix_blocks_apply_and_preserves_outside_file(
    tmp_path: Path,
) -> None:
    root = tmp_path / "research" / "raw"
    reference, artifact_path = _artifact(root, {"outside": "must-remain"})
    outside = tmp_path / "outside"
    outside.mkdir()
    outside_artifact = outside / artifact_path.name
    shutil.copyfile(artifact_path, outside_artifact)
    artifact_path.unlink()
    artifact_path.parent.rmdir()
    try:
        artifact_path.parent.symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("directory symlinks require platform support")
    record = _record("daily", reference, observed_at=NOW - timedelta(days=31))

    result = execute_raw_retention((record,), root, as_of=NOW, apply=True)

    assert result.plan.blocked is True
    assert result.deleted_paths == ()
    assert any("escapes configured root" in error for error in result.plan.blocking_errors)
    assert outside_artifact.exists()


def test_damaged_raw_reference_json_blocks_the_entire_apply(tmp_path: Path) -> None:
    root = tmp_path / "research" / "raw"
    reference, artifact_path = _artifact(root, {"valid": True})
    records = (
        _record("daily", reference, observed_at=NOW - timedelta(days=31), record_id=1),
        _record("daily", "{not-json", observed_at=NOW - timedelta(days=31), record_id=2),
    )

    result = execute_raw_retention(records, root, as_of=NOW, apply=True)

    assert result.blocked is True
    assert result.deleted_paths == ()
    assert any("not valid JSON" in error for error in result.plan.blocking_errors)
    assert artifact_path.exists()


def test_missing_permanent_reference_blocks_before_expired_candidate_moves(
    tmp_path: Path,
) -> None:
    root = tmp_path / "research" / "raw"
    expired_ref, expired_path = _artifact(root, {"expired": True})
    permanent_ref, permanent_path = _artifact(root, {"permanent": True})
    permanent_path.unlink()
    records = (
        _record("daily", expired_ref, observed_at=NOW - timedelta(days=31), record_id=1),
        _record("income", permanent_ref, observed_at=NOW, record_id=2),
    )

    result = execute_raw_retention(records, root, as_of=NOW, apply=True)

    assert result.plan.blocked is True
    assert result.deleted_paths == ()
    assert any("protected reference" in error for error in result.plan.blocking_errors)
    assert expired_path.exists()


def test_corrupt_gzip_or_hash_blocks_the_entire_apply(tmp_path: Path) -> None:
    root = tmp_path / "research" / "raw"
    reference, artifact_path = _artifact(root, {"forensic": "payload"})
    artifact_path.write_bytes(b"not-gzip-and-not-the-referenced-content")
    record = _record("daily", reference, observed_at=NOW - timedelta(days=31))

    result = execute_raw_retention((record,), root, as_of=NOW, apply=True)

    assert result.plan.blocked is True
    assert result.deleted_paths == ()
    assert any("not readable gzip" in error for error in result.plan.blocking_errors)
    assert artifact_path.exists()


def test_recent_job_binding_extends_deduplicated_market_raw_retention(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "stock_analysis.db"
    root = tmp_path / "research" / "raw"
    reference, artifact_path = _artifact(root, {"unchanged": "provider-response"})
    content_hash = _create_cli_database(
        database_path,
        [("daily", reference, NOW - timedelta(days=31), {"close": 1.0})],
    )[0]
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            INSERT INTO job_events (event_type, payload_json, created_at)
            VALUES ('research_dataset_snapshot', ?, ?)
            """,
            (json.dumps({"content_hash": content_hash}), NOW.isoformat()),
        )
        records = raw_retention.load_snapshot_records(connection)

    plan = build_raw_retention_plan(records, root, as_of=NOW)

    assert plan.blocked is False
    assert plan.candidate_paths == ()
    assert artifact_path.exists()


def test_external_shared_raw_root_is_dry_run_only_for_cli_apply(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    database_path = tmp_path / "backup" / "stock_analysis.db"
    database_path.parent.mkdir()
    shared_root = tmp_path / "production" / "research" / "raw"
    reference, artifact_path = _artifact(shared_root, {"live-in-production": True})
    _create_cli_database(
        database_path,
        [("daily", reference, NOW - timedelta(days=31), {"close": 1.0})],
    )
    monkeypatch.setenv("DATABASE_PATH", str(database_path))
    args = [
        "--database-path",
        str(database_path),
        "--raw-root",
        str(shared_root),
        "--as-of",
        NOW.isoformat(),
    ]

    assert main(args) == 0
    assert json.loads(capsys.readouterr().out)["mode"] == "dry-run"
    assert main([*args, "--apply"]) == 1
    error = json.loads(capsys.readouterr().out)["error"]

    assert "external/shared --raw-root is dry-run only" in error
    assert artifact_path.exists()


def test_same_directory_backup_cannot_apply_against_active_production_raw(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    production_database = data_dir / "production.db"
    backup_database = data_dir / "backup.sqlite"
    shared_root = data_dir / "research" / "raw"
    reference, artifact_path = _artifact(shared_root, {"live-in-production": True})
    _create_cli_database(
        production_database,
        [("daily", reference, NOW, {"close": 1.0})],
    )
    _create_cli_database(
        backup_database,
        [("daily", reference, NOW - timedelta(days=31), {"close": 1.0})],
    )
    monkeypatch.setenv("DATABASE_PATH", str(production_database))

    exit_code = main(
        [
            "--database-path",
            str(backup_database),
            "--as-of",
            NOW.isoformat(),
            "--apply",
        ]
    )

    assert exit_code == 1
    assert "active DATABASE_PATH" in json.loads(capsys.readouterr().out)["error"]
    assert artifact_path.exists()


def test_relative_active_database_path_matches_explicit_absolute_apply(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    database_path = tmp_path / "stock_analysis.db"
    root = tmp_path / "research" / "raw"
    reference, artifact_path = _artifact(root, {"relative-active-path": True})
    _create_cli_database(
        database_path,
        [("daily", reference, NOW - timedelta(days=31), {"close": 1.0})],
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DATABASE_PATH", "stock_analysis.db")

    exit_code = main(
        [
            "--database-path",
            str(database_path.resolve()),
            "--as-of",
            NOW.isoformat(),
            "--apply",
        ]
    )

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out)["mode"] == "apply"
    assert not artifact_path.exists()


def test_missing_active_database_fails_closed_before_selected_copy_is_applied(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    selected_database = tmp_path / "isolated" / "stock_analysis.db"
    selected_database.parent.mkdir()
    root = selected_database.parent / "research" / "raw"
    reference, artifact_path = _artifact(root, {"selected-copy": True})
    _create_cli_database(
        selected_database,
        [("daily", reference, NOW - timedelta(days=31), {"close": 1.0})],
    )
    monkeypatch.setenv("DATABASE_PATH", str(tmp_path / "missing-production.db"))

    exit_code = main(
        [
            "--database-path",
            str(selected_database),
            "--as-of",
            NOW.isoformat(),
            "--apply",
        ]
    )

    assert exit_code == 1
    assert "active DATABASE_PATH does not exist" in json.loads(
        capsys.readouterr().out
    )["error"]
    assert artifact_path.exists()


def test_apply_refuses_while_durable_research_worker_owns_provider(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    database_path = tmp_path / "stock_analysis.db"
    root = tmp_path / "research" / "raw"
    reference, artifact_path = _artifact(root, {"worker-owned": True})
    _create_cli_database(
        database_path,
        [("daily", reference, NOW - timedelta(days=31), {"close": 1.0})],
    )
    monkeypatch.setenv("DATABASE_PATH", str(database_path))
    owner = TushareWorkerOwnerLock.from_database_url(
        raw_retention._database_url(database_path)
    ).acquire()
    try:
        result = main(
            [
                "--database-path",
                str(database_path),
                "--as-of",
                NOW.isoformat(),
                "--apply",
            ]
        )
    finally:
        owner.release()

    assert result == 1
    assert "another Durable Worker" in json.loads(capsys.readouterr().out)["error"]
    assert artifact_path.exists()


def test_database_lock_timeout_releases_owner_and_never_moves_raw(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    database_path = tmp_path / "stock_analysis.db"
    root = tmp_path / "research" / "raw"
    reference, artifact_path = _artifact(root, {"db-locked": True})
    _create_cli_database(
        database_path,
        [("daily", reference, NOW - timedelta(days=31), {"close": 1.0})],
    )
    monkeypatch.setenv("DATABASE_PATH", str(database_path))
    holder = sqlite3.connect(database_path)
    holder.execute("BEGIN IMMEDIATE")
    real_connect = raw_retention.sqlite3.connect

    class _ShortBusyConnection:
        def __init__(self, connection: sqlite3.Connection) -> None:
            self.connection = connection

        def execute(self, statement, *args, **kwargs):
            if str(statement).startswith("PRAGMA busy_timeout"):
                return self.connection.execute("PRAGMA busy_timeout = 1")
            return self.connection.execute(statement, *args, **kwargs)

        def rollback(self) -> None:
            self.connection.rollback()

        def close(self) -> None:
            self.connection.close()

    def short_busy_connect(*args, **kwargs):
        return _ShortBusyConnection(real_connect(*args, **kwargs))

    try:
        with patch.object(raw_retention.sqlite3, "connect", side_effect=short_busy_connect):
            exit_code = main(
                [
                    "--database-path",
                    str(database_path),
                    "--as-of",
                    NOW.isoformat(),
                    "--apply",
                ]
            )
    finally:
        holder.rollback()
        holder.close()

    assert exit_code == 1
    assert "locked" in json.loads(capsys.readouterr().out)["error"].lower()
    assert artifact_path.exists()
    owner = TushareWorkerOwnerLock.from_database_url(
        raw_retention._database_url(database_path)
    ).acquire()
    owner.release()


def test_staging_failure_restores_every_canonical_candidate(tmp_path: Path) -> None:
    root = tmp_path / "research" / "raw"
    artifacts = [_artifact(root, {"candidate": index}) for index in range(2)]
    records = tuple(
        _record(
            "daily",
            reference,
            observed_at=NOW - timedelta(days=31),
            record_id=index,
        )
        for index, (reference, _path) in enumerate(artifacts, start=1)
    )
    real_replace = raw_retention.os.replace
    stage_moves = 0

    def fail_second_stage(source, destination):
        nonlocal stage_moves
        if raw_retention._STAGING_PREFIX in str(destination):
            stage_moves += 1
            if stage_moves == 2:
                raise OSError("injected stage failure")
        return real_replace(source, destination)

    with patch.object(raw_retention.os, "replace", side_effect=fail_second_stage):
        result = execute_raw_retention(records, root, as_of=NOW, apply=True)

    assert result.mode == "apply_failed"
    assert result.deleted_paths == ()
    assert result.apply_errors
    assert all(path.exists() for _reference, path in artifacts)
    assert list(root.glob(f"{raw_retention._STAGING_PREFIX}*")) == []


def test_purge_failure_is_committed_and_reports_recoverable_staging(
    tmp_path: Path,
) -> None:
    root = tmp_path / "research" / "raw"
    artifacts = [_artifact(root, {"purge": index}) for index in range(2)]
    records = tuple(
        _record(
            "daily",
            reference,
            observed_at=NOW - timedelta(days=31),
            record_id=index,
        )
        for index, (reference, _path) in enumerate(artifacts, start=1)
    )
    real_unlink = Path.unlink
    purge_calls = 0

    def fail_second_purge(path: Path, *args, **kwargs):
        nonlocal purge_calls
        if raw_retention._STAGING_PREFIX in str(path):
            purge_calls += 1
            if purge_calls == 2:
                raise OSError("injected purge failure")
        return real_unlink(path, *args, **kwargs)

    with patch.object(Path, "unlink", autospec=True, side_effect=fail_second_purge):
        result = execute_raw_retention(records, root, as_of=NOW, apply=True)

    assert result.mode == "committed_with_cleanup_error"
    assert result.apply_errors
    assert len(result.deleted_paths) == 2
    assert len(result.cleanup_pending_paths) == 1
    assert result.staging_directory is not None
    assert Path(result.staging_directory).is_dir()
    assert all(not path.exists() for _reference, path in artifacts)


def test_cli_committed_cleanup_error_returns_one_not_preflight_two(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    database_path = tmp_path / "stock_analysis.db"
    root = tmp_path / "research" / "raw"
    entries: list[tuple[str, dict[str, object], datetime, object]] = []
    artifact_paths: list[Path] = []
    for index in range(2):
        reference, artifact_path = _artifact(root, {"cli-purge": index})
        entries.append(
            ("daily", reference, NOW - timedelta(days=31), {"close": index})
        )
        artifact_paths.append(artifact_path)
    _create_cli_database(database_path, entries)
    monkeypatch.setenv("DATABASE_PATH", str(database_path))
    real_unlink = Path.unlink
    failed = False

    def fail_first_purge(path: Path, *args, **kwargs):
        nonlocal failed
        if raw_retention._STAGING_PREFIX in str(path) and not failed:
            failed = True
            raise OSError("injected cli purge failure")
        return real_unlink(path, *args, **kwargs)

    with patch.object(Path, "unlink", autospec=True, side_effect=fail_first_purge):
        exit_code = main(
            [
                "--database-path",
                str(database_path),
                "--as-of",
                NOW.isoformat(),
                "--apply",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["mode"] == "committed_with_cleanup_error"
    assert payload["cleanup_pending_paths"]
    assert all(not path.exists() for path in artifact_paths)


def test_database_finalize_failure_preserves_committed_deletion_result(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    database_path = tmp_path / "stock_analysis.db"
    root = tmp_path / "research" / "raw"
    reference, artifact_path = _artifact(root, {"finalize-failure": True})
    _create_cli_database(
        database_path,
        [("daily", reference, NOW - timedelta(days=31), {"close": 1.0})],
    )
    monkeypatch.setenv("DATABASE_PATH", str(database_path))
    real_connect = raw_retention.sqlite3.connect

    class _FinalizeFailureConnection:
        def __init__(self, connection: sqlite3.Connection) -> None:
            self.connection = connection

        def execute(self, statement, *args, **kwargs):
            return self.connection.execute(statement, *args, **kwargs)

        def rollback(self) -> None:
            raise sqlite3.OperationalError("injected transaction release failure")

        def close(self) -> None:
            self.connection.close()

    def finalize_failure_connect(*args, **kwargs):
        return _FinalizeFailureConnection(real_connect(*args, **kwargs))

    with patch.object(
        raw_retention.sqlite3,
        "connect",
        side_effect=finalize_failure_connect,
    ):
        exit_code = main(
            [
                "--database-path",
                str(database_path),
                "--as-of",
                NOW.isoformat(),
                "--apply",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["mode"] == "committed_with_cleanup_error"
    assert payload["deleted_paths"] == [str(reference["relative_path"])]
    assert "transaction release" in payload["apply_errors"][0]
    assert not artifact_path.exists()


def test_leftover_staging_blocks_a_later_apply_without_new_moves(tmp_path: Path) -> None:
    root = tmp_path / "research" / "raw"
    reference, artifact_path = _artifact(root, {"candidate": "blocked-by-staging"})
    stale = root / f"{raw_retention._STAGING_PREFIX}interrupted"
    stale.mkdir()
    record = _record("daily", reference, observed_at=NOW - timedelta(days=31))

    result = execute_raw_retention((record,), root, as_of=NOW, apply=True)

    assert result.mode == "apply_failed"
    assert result.deleted_paths == ()
    assert result.staging_directory == str(stale)
    assert artifact_path.exists()


def test_cli_rejects_naive_as_of_before_opening_database(
    tmp_path: Path,
    capsys,
) -> None:
    database_path = tmp_path / "stock_analysis.db"
    database_path.touch()

    assert main(
        [
            "--database-path",
            str(database_path),
            "--as-of",
            "2026-08-08T08:00:00",
        ]
    ) == 1

    assert "must include Z or an explicit UTC offset" in json.loads(
        capsys.readouterr().out
    )["error"]


def test_default_database_path_loads_env_file(tmp_path: Path, monkeypatch) -> None:
    configured = tmp_path / "configured.sqlite"
    env_file = tmp_path / "retention.env"
    env_file.write_text(f"DATABASE_PATH={configured.as_posix()}\n", encoding="utf-8")
    monkeypatch.setenv("ENV_FILE", str(env_file))
    monkeypatch.delenv("DATABASE_PATH", raising=False)

    assert raw_retention._default_database_path() == configured


def test_cli_defaults_to_dry_run_and_leaves_normalized_snapshot_self_contained(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    database_path = tmp_path / "stock_analysis.db"
    root = tmp_path / "research" / "raw"
    reference, artifact_path = _artifact(root, {"provider": "raw"})
    _create_cli_database(
        database_path,
        [
            (
                "daily",
                reference,
                NOW - timedelta(days=31),
                {"close": 123.45},
            )
        ],
    )
    monkeypatch.setenv("DATABASE_PATH", str(database_path))

    common_args = [
        "--database-path",
        str(database_path),
        "--raw-root",
        str(root),
        "--as-of",
        NOW.isoformat(),
    ]
    assert main(common_args) == 0
    dry_payload = json.loads(capsys.readouterr().out)
    assert dry_payload["mode"] == "dry-run"
    assert dry_payload["deleted_paths"] == []
    assert artifact_path.exists()

    assert main([*common_args, "--apply"]) == 0
    apply_payload = json.loads(capsys.readouterr().out)
    assert apply_payload["mode"] == "apply"
    assert apply_payload["deleted_paths"] == [str(reference["relative_path"])]
    assert not artifact_path.exists()

    connection = sqlite3.connect(database_path)
    stored = connection.execute(
        "SELECT normalized_json, raw_ref_json FROM research_dataset_snapshots WHERE id = 1"
    ).fetchone()
    connection.close()
    assert json.loads(stored[0]) == {"close": 123.45}
    assert json.loads(stored[1]) == reference
