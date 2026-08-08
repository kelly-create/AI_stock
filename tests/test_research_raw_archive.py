from __future__ import annotations

import gzip
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import tarfile
import time

import pytest

from scripts import research_raw_archive
from scripts import sqlite_backup
from src.services.research.raw_store import RawArtifactStore


def _write_artifact(root: Path, payload: bytes, *, extension: str = "json") -> dict[str, object]:
    digest = hashlib.sha256(payload).hexdigest()
    relative_path = f"{digest[:2]}/{digest}.{extension}.gz"
    target = root / relative_path
    target.parent.mkdir(parents=True, exist_ok=True)
    compressed = gzip.compress(payload, mtime=0)
    target.write_bytes(compressed)
    return {
        "content_sha256": digest,
        "relative_path": relative_path,
        "compression": "gzip",
        "media_type": "application/json",
        "uncompressed_bytes": len(payload),
        "compressed_bytes": len(compressed),
    }


def _write_database(path: Path, references: list[dict[str, object]]) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE schema_migrations (version TEXT PRIMARY KEY)")
        connection.execute("INSERT INTO schema_migrations(version) VALUES ('test_research_raw_archive')")
        connection.execute(
            "CREATE TABLE research_dataset_snapshots ("
            "id INTEGER PRIMARY KEY, raw_ref_json TEXT NULL)"
        )
        connection.executemany(
            "INSERT INTO research_dataset_snapshots(raw_ref_json) VALUES (?)",
            [(json.dumps(reference, sort_keys=True),) for reference in references],
        )
        connection.execute("INSERT INTO research_dataset_snapshots(raw_ref_json) VALUES (NULL)")


def _create_bundle(tmp_path: Path) -> tuple[Path, Path, Path, Path, dict[str, object]]:
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    referenced = _write_artifact(raw_root, b'{"provider":"tushare","rows":[1]}')
    _write_artifact(raw_root, b'{"orphan":true}', extension="bin")
    database = tmp_path / "stock-analysis.sqlite"
    _write_database(database, [referenced, referenced])
    archive = tmp_path / "research-raw.tar"
    manifest = research_raw_archive.create_raw_archive(raw_root, database, archive)
    return raw_root, database, archive, research_raw_archive._default_manifest_path(archive), manifest


def _rewrite_manifest(path: Path, payload: dict[str, object]) -> None:
    payload["manifest_sha256"] = research_raw_archive._manifest_canonical_sha256(payload)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def test_cli_full_forensic_restore_drill_reports_elapsed_and_meets_rto(tmp_path: Path, capsys) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    first = _write_artifact(raw_root, b'{"daily":[1,2,3]}')
    second = _write_artifact(raw_root, b'{"stock_basic":[4]}')
    _write_artifact(raw_root, b'{"unreferenced":"still archived"}', extension="txt")
    live_database = tmp_path / "live.sqlite"
    _write_database(live_database, [first, second])
    database = tmp_path / "backup.sqlite"
    sqlite_backup.create_backup(
        live_database,
        database,
        core_tables=("research_dataset_snapshots",),
    )
    archive = tmp_path / "raw-evidence.tar"
    restore_root = tmp_path / "isolated-restore"
    restored_database = restore_root / "stock_analysis.db"
    restored = restore_root / "research" / "raw"
    restored.parent.mkdir(parents=True)

    assert research_raw_archive.main(
        [
            "create",
            "--raw-root",
            str(raw_root),
            "--database-backup",
            str(database),
            "--archive",
            str(archive),
        ]
    ) == 0
    create_result = json.loads(capsys.readouterr().out)
    assert create_result["status"] == "created"
    assert create_result["artifact_count"] == 3
    assert create_result["referenced_artifact_count"] == 2
    assert 0 <= create_result["elapsed_seconds"] < 900

    # RTO starts once the complete bundle is locally available. Exercise the
    # actual SQLite verifier/restorer, then verify the raw binding against the
    # differently named restored database before restoring raw artifacts.
    drill_started = time.monotonic()
    sqlite_backup.verify_backup(database)
    assert research_raw_archive.main(
        ["verify", "--archive", str(archive), "--database-backup", str(database)]
    ) == 0
    verify_result = json.loads(capsys.readouterr().out)
    assert verify_result["status"] == "verified"
    assert 0 <= verify_result["elapsed_seconds"] < 900

    sqlite_result = sqlite_backup.restore_backup(database, restored_database)
    assert sqlite_result["status"] == "restored"
    assert restored_database.name != database.name
    assert research_raw_archive.main(
        ["verify", "--archive", str(archive), "--database-backup", str(restored_database)]
    ) == 0
    renamed_verify_result = json.loads(capsys.readouterr().out)
    assert renamed_verify_result["status"] == "verified"

    assert research_raw_archive.main(
        [
            "restore",
            "--archive",
            str(archive),
            "--database-backup",
            str(restored_database),
            "--target-root",
            str(restored),
        ]
    ) == 0
    restore_result = json.loads(capsys.readouterr().out)
    assert restore_result["status"] == "restored"
    assert restore_result["artifact_count"] == 3
    assert 0 <= restore_result["elapsed_seconds"] < 900
    assert research_raw_archive.verify_raw_archive(archive, restored_database)["archive"]["name"] == archive.name

    store = RawArtifactStore(restored)
    assert store.read(first) == b'{"daily":[1,2,3]}'
    assert store.read(second) == b'{"stock_basic":[4]}'
    total_restore_seconds = time.monotonic() - drill_started
    assert total_restore_seconds < 900

    assert sorted(path.relative_to(restored).as_posix() for path in restored.rglob("*.gz")) == sorted(
        path.relative_to(raw_root).as_posix() for path in raw_root.rglob("*.gz")
    )
    for source in raw_root.rglob("*.gz"):
        target = restored / source.relative_to(raw_root)
        assert target.read_bytes() == source.read_bytes()


@pytest.mark.parametrize(
    "relative_path",
    (
        ".retention-staging-deadbeef/leftover.json.gz",
        ".012345.tmp",
        "notes.txt",
        "aa/nested/unexpected.gz",
    ),
)
def test_create_rejects_noncanonical_staging_temp_and_nested_entries(
    tmp_path: Path,
    relative_path: str,
) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    candidate = raw_root / relative_path
    if relative_path.endswith("unexpected.gz"):
        candidate.parent.mkdir(parents=True)
        candidate.write_bytes(b"unexpected")
    elif "/" in relative_path:
        candidate.parent.mkdir(parents=True)
        candidate.write_bytes(b"staging")
    else:
        candidate.write_bytes(b"temporary")
    database = tmp_path / "backup.sqlite"
    _write_database(database, [])
    archive = tmp_path / "raw.tar"

    with pytest.raises(research_raw_archive.RawArchiveError, match="noncanonical"):
        research_raw_archive.create_raw_archive(raw_root, database, archive)

    assert not archive.exists()
    assert not research_raw_archive._default_manifest_path(archive).exists()
    assert not list(tmp_path.glob(".raw.tar.*.tmp"))


def test_create_rejects_symlink_anywhere_in_raw_root(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    link = raw_root / "aa"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symbolic links are unavailable on this platform")
    database = tmp_path / "backup.sqlite"
    _write_database(database, [])

    with pytest.raises(research_raw_archive.RawArchiveError, match="symbolic link"):
        research_raw_archive.create_raw_archive(raw_root, database, tmp_path / "raw.tar")


def test_create_rejects_corrupt_gzip_and_cleans_outputs(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    digest = hashlib.sha256(b"expected").hexdigest()
    target = raw_root / digest[:2] / f"{digest}.json.gz"
    target.parent.mkdir()
    target.write_bytes(b"not-gzip")
    database = tmp_path / "backup.sqlite"
    _write_database(database, [])
    archive = tmp_path / "raw.tar"

    with pytest.raises(research_raw_archive.RawArchiveError, match="gzip"):
        research_raw_archive.create_raw_archive(raw_root, database, archive)

    assert not archive.exists()
    assert not research_raw_archive._default_manifest_path(archive).exists()
    assert not list(tmp_path.glob(".*.tmp"))


def test_create_supports_long_canonical_raw_store_extension(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    # The member name exceeds the USTAR 100-byte limit while remaining below
    # Windows' legacy full-path limit in the pytest temporary directory.
    reference = _write_artifact(raw_root, b"long-extension", extension="x" * 40)
    database = tmp_path / "backup.sqlite"
    _write_database(database, [reference])
    archive = tmp_path / "raw.tar"

    manifest = research_raw_archive.create_raw_archive(raw_root, database, archive)

    assert manifest["references"]["artifact_count"] == 1
    assert research_raw_archive.verify_raw_archive(archive, database) == manifest


def test_create_rejects_sqlite_reference_missing_from_raw_root(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    missing_payload = b"missing"
    digest = hashlib.sha256(missing_payload).hexdigest()
    reference = {
        "content_sha256": digest,
        "relative_path": f"{digest[:2]}/{digest}.json.gz",
        "compression": "gzip",
        "compressed_bytes": len(gzip.compress(missing_payload, mtime=0)),
        "uncompressed_bytes": len(missing_payload),
    }
    database = tmp_path / "backup.sqlite"
    _write_database(database, [reference])
    archive = tmp_path / "raw.tar"

    with pytest.raises(research_raw_archive.RawArchiveError, match="missing from the archive"):
        research_raw_archive.create_raw_archive(raw_root, database, archive)

    assert not archive.exists()
    assert not research_raw_archive._default_manifest_path(archive).exists()


def test_create_never_overwrites_existing_output(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    database = tmp_path / "backup.sqlite"
    _write_database(database, [])
    archive = tmp_path / "raw.tar"
    archive.write_bytes(b"existing-evidence")

    with pytest.raises(research_raw_archive.RawArchiveError, match="already exists"):
        research_raw_archive.create_raw_archive(raw_root, database, archive)

    assert archive.read_bytes() == b"existing-evidence"
    assert not research_raw_archive._default_manifest_path(archive).exists()


def test_failed_manifest_publication_removes_half_published_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    database = tmp_path / "backup.sqlite"
    _write_database(database, [])
    archive = tmp_path / "raw.tar"
    manifest = research_raw_archive._default_manifest_path(archive)
    real_publish = research_raw_archive._atomic_publish_new
    calls = 0

    def fail_second(source: Path, destination: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected manifest publication failure")
        real_publish(source, destination)

    monkeypatch.setattr(research_raw_archive, "_atomic_publish_new", fail_second)
    with pytest.raises(research_raw_archive.RawArchiveError, match="publication failed"):
        research_raw_archive.create_raw_archive(raw_root, database, archive)

    assert not archive.exists()
    assert not manifest.exists()
    assert not list(tmp_path.glob(".*.tmp"))


def test_create_rejects_oversized_manifest_before_publication_and_cleans_temps(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    database = tmp_path / "backup.sqlite"
    _write_database(database, [])
    archive = tmp_path / "raw.tar"
    manifest = research_raw_archive._default_manifest_path(archive)
    monkeypatch.setattr(research_raw_archive, "MAX_MANIFEST_BYTES", 1)

    with pytest.raises(research_raw_archive.RawArchiveError, match="manifest is too large"):
        research_raw_archive.create_raw_archive(raw_root, database, archive)

    assert not archive.exists()
    assert not manifest.exists()
    assert not list(tmp_path.glob(".*.tmp"))


@pytest.mark.parametrize("tamper", ("archive", "database", "manifest"))
def test_verify_rejects_any_bundle_file_tamper(tmp_path: Path, tamper: str) -> None:
    _, database, archive, manifest_path, _ = _create_bundle(tmp_path)
    if tamper == "archive":
        with archive.open("r+b") as handle:
            first = handle.read(1)
            handle.seek(0)
            handle.write(bytes([first[0] ^ 0x01]))
    elif tamper == "database":
        with database.open("ab") as handle:
            handle.write(b"tampered")
    else:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        payload["created_at_utc"] = "2026-08-09T00:00:00Z"
        manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(research_raw_archive.RawArchiveError):
        research_raw_archive.verify_raw_archive(archive, database)


def test_verify_rejects_path_traversal_even_with_rehashed_archive_and_manifest(tmp_path: Path) -> None:
    raw_root, database, archive, manifest_path, _ = _create_bundle(tmp_path)
    source = next(raw_root.rglob("*.gz"))
    malicious = tmp_path / "malicious.tar"
    with tarfile.open(malicious, mode="w:", format=tarfile.USTAR_FORMAT) as bundle:
        info = tarfile.TarInfo("../escape.json.gz")
        data = source.read_bytes()
        info.size = len(data)
        info.mode = 0o600
        info.mtime = 0
        info.uid = info.gid = 0
        info.uname = info.gname = ""
        bundle.addfile(info, io.BytesIO(data))
    archive.unlink()
    malicious.replace(archive)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["archive"]["sha256"] = research_raw_archive._sha256_file(archive)
    payload["archive"]["size_bytes"] = archive.stat().st_size
    _rewrite_manifest(manifest_path, payload)

    with pytest.raises(research_raw_archive.RawArchiveError, match="canonical"):
        research_raw_archive.verify_raw_archive(archive, database)

    assert not (tmp_path / "escape.json.gz").exists()


def test_verify_rejects_archive_missing_a_database_referenced_artifact(tmp_path: Path) -> None:
    _, database, archive, manifest_path, _ = _create_bundle(tmp_path)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    referenced_path = payload["references"]["artifacts"][0]["relative_path"]
    retained_members: list[tuple[str, bytes]] = []
    with tarfile.open(archive, mode="r:") as source:
        for member in source.getmembers():
            if member.name == referenced_path:
                continue
            handle = source.extractfile(member)
            assert handle is not None
            retained_members.append((member.name, handle.read()))
    replacement = tmp_path / "replacement.tar"
    with tarfile.open(replacement, mode="w:", format=tarfile.GNU_FORMAT) as target:
        for name, data in retained_members:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mode = 0o600
            info.mtime = 0
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            target.addfile(info, io.BytesIO(data))
    archive.unlink()
    replacement.replace(archive)
    payload["archive"]["sha256"] = research_raw_archive._sha256_file(archive)
    payload["archive"]["size_bytes"] = archive.stat().st_size
    payload["artifacts"] = [
        item for item in payload["artifacts"] if item["relative_path"] != referenced_path
    ]
    _rewrite_manifest(manifest_path, payload)

    with pytest.raises(research_raw_archive.RawArchiveError, match="absent from its file list"):
        research_raw_archive.verify_raw_archive(archive, database)


def test_verify_rejects_database_reference_set_changed_after_archive(tmp_path: Path) -> None:
    _, database, archive, manifest_path, _ = _create_bundle(tmp_path)
    with sqlite3.connect(database) as connection:
        connection.execute("DELETE FROM research_dataset_snapshots WHERE raw_ref_json IS NOT NULL")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["database_backup"]["sha256"] = research_raw_archive._sha256_file(database)
    payload["database_backup"]["size_bytes"] = database.stat().st_size
    _rewrite_manifest(manifest_path, payload)

    with pytest.raises(research_raw_archive.RawArchiveError, match="references do not match"):
        research_raw_archive.verify_raw_archive(archive, database)


def test_restore_refuses_existing_target_without_changing_it(tmp_path: Path) -> None:
    _, database, archive, _, _ = _create_bundle(tmp_path)
    target = tmp_path / "isolated-raw"
    target.mkdir()
    marker = target / "keep.txt"
    marker.write_bytes(b"do-not-overwrite")

    with pytest.raises(research_raw_archive.RawArchiveError, match="already exists"):
        research_raw_archive.restore_raw_archive(archive, database, target)

    assert marker.read_bytes() == b"do-not-overwrite"


def test_restore_publication_failure_removes_claimed_target_and_staging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_root, database, archive, _, _ = _create_bundle(tmp_path)
    # Ensure publication would have at least two shard moves before injecting
    # the failure, so cleanup covers a genuinely partial target tree.
    existing_shards = {path.parent.name for path in raw_root.rglob("*.gz")}
    counter = 0
    while len(existing_shards) < 2:
        _write_artifact(raw_root, f"extra-{counter}".encode())
        counter += 1
        existing_shards = {path.parent.name for path in raw_root.rglob("*.gz")}
    archive.unlink()
    research_raw_archive._default_manifest_path(archive).unlink()
    research_raw_archive.create_raw_archive(raw_root, database, archive)
    target = tmp_path / "restored-raw"
    real_move = research_raw_archive._move_restore_shard
    moves = 0

    def fail_second_move(source: Path, destination: Path) -> None:
        nonlocal moves
        moves += 1
        if moves == 2:
            raise OSError("injected restore publication failure")
        real_move(source, destination)

    monkeypatch.setattr(research_raw_archive, "_move_restore_shard", fail_second_move)
    with pytest.raises(research_raw_archive.RawArchiveError, match="publication failed"):
        research_raw_archive.restore_raw_archive(archive, database, target)

    assert not target.exists()
    assert not list(tmp_path.glob(".restored-raw.restore-*"))


def test_restore_race_does_not_delete_or_overwrite_competing_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, database, archive, _, _ = _create_bundle(tmp_path)
    target = tmp_path / "restored-raw"
    real_mkdtemp = research_raw_archive.tempfile.mkdtemp

    def plant_target(*args, **kwargs) -> str:
        staging = real_mkdtemp(*args, **kwargs)
        target.mkdir()
        (target / "competitor.txt").write_bytes(b"preserve")
        return staging

    monkeypatch.setattr(research_raw_archive.tempfile, "mkdtemp", plant_target)
    with pytest.raises(research_raw_archive.RawArchiveError, match="publication failed"):
        research_raw_archive.restore_raw_archive(archive, database, target)

    assert (target / "competitor.txt").read_bytes() == b"preserve"
    assert not list(tmp_path.glob(".restored-raw.restore-*"))
