#!/usr/bin/env python3
"""Create, verify, and restore integrity-checked SQLite online backups."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

try:
    from scripts.capture_production_baseline import (
        DEFAULT_CORE_TABLES,
        BaselineError,
        evaluate_core_table_acceptance,
        inspect_sqlite_database,
        sha256_file,
    )
except ModuleNotFoundError:  # pragma: no cover - direct ``python scripts/...`` execution
    from capture_production_baseline import (  # type: ignore[no-redef]
        DEFAULT_CORE_TABLES,
        BaselineError,
        evaluate_core_table_acceptance,
        inspect_sqlite_database,
        sha256_file,
    )


FORMAT_VERSION = 1
MANIFEST_KIND = "dsa-sqlite-online-backup"
MAX_MANIFEST_BYTES = 2 * 1024 * 1024
SQLITE_SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")
DEFAULT_BACKUP_CORE_TABLES = DEFAULT_CORE_TABLES + (
    "analysis_jobs",
    "job_events",
    "notification_outbox",
    "provider_health",
    "research_dataset_snapshots",
    "research_factor_snapshots",
    "research_evidence_snapshots",
    "research_snapshots",
)


class BackupError(RuntimeError):
    """Raised when a backup operation cannot complete safely."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _filename_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _quoted_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _path_exists(path: Path) -> bool:
    return os.path.lexists(path)


def _require_regular_file(path: Path, label: str) -> None:
    if path.is_symlink() or not path.is_file():
        raise BackupError(f"{label} must be a regular file")


def _reject_symlink(path: Path, label: str) -> None:
    if path.is_symlink():
        raise BackupError(f"{label} must not be a symlink")


def _require_existing_parent(path: Path, label: str) -> None:
    if not path.parent.is_dir():
        raise BackupError(f"{label} parent directory must already exist")
    if path.parent.is_symlink():
        raise BackupError(f"{label} parent directory must not be a symlink")


def _deduplicate_core_tables(core_tables: Sequence[str]) -> tuple[str, ...]:
    result: list[str] = []
    for table in core_tables:
        if not isinstance(table, str) or not table.strip():
            raise BackupError("core table names must be non-empty strings")
        if table not in result:
            result.append(table)
    if not result:
        raise BackupError("at least one core table is required")
    return tuple(result)


def _canonical_sha256(value: Any) -> str:
    rendered = json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(rendered).hexdigest()


def _manifest_canonical_sha256(manifest: dict[str, Any]) -> str:
    canonical_payload = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    return _canonical_sha256(canonical_payload)


def _read_schema_migrations(database_path: Path) -> dict[str, Any]:
    uri = f"{database_path.resolve().as_uri()}?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=5.0)
        connection.execute("PRAGMA query_only=ON")
        row = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'"
        ).fetchone()
        if row is None:
            raise BackupError("schema_migrations table is missing")
        columns = {str(item[1]) for item in connection.execute("PRAGMA table_info(schema_migrations)").fetchall()}
        if "version" not in columns:
            raise BackupError("schema_migrations.version is missing")
        rows = connection.execute(
            f"SELECT {_quoted_identifier('version')} FROM {_quoted_identifier('schema_migrations')} "
            f"ORDER BY {_quoted_identifier('version')}"
        ).fetchall()
    except sqlite3.Error as exc:
        raise BackupError("unable to read schema migration versions") from exc
    finally:
        if "connection" in locals():
            connection.close()

    versions = []
    for row in rows:
        value = row[0]
        if value is None or isinstance(value, (bytes, bytearray, memoryview)):
            raise BackupError("schema migration versions must be non-empty text values")
        version = str(value)
        if not version:
            raise BackupError("schema migration versions must be non-empty text values")
        versions.append(version)
    return {"count": len(versions), "versions": versions, "canonical_sha256": _canonical_sha256(versions)}


def _database_metadata(database_path: Path, core_tables: Sequence[str]) -> dict[str, Any]:
    required_tables = _deduplicate_core_tables(core_tables)
    try:
        snapshot = inspect_sqlite_database(database_path, required_tables)
    except BaselineError as exc:
        raise BackupError("database inspection failed") from exc

    acceptance = evaluate_core_table_acceptance(snapshot, required_tables, strict=True)
    if acceptance["blocking"]:
        raise BackupError("database integrity or required-table checks failed")

    index_objects = snapshot["indexes"]["objects"]
    counts = {
        table: int(snapshot["core_table_counts"][table]["count"])
        for table in required_tables
    }
    return {
        "schema_migrations": _read_schema_migrations(database_path),
        "schema": {
            "object_count": int(snapshot["schema"]["object_count"]),
            "canonical_sha256": str(snapshot["schema"]["sha256"]),
        },
        "indexes": {
            "count": int(snapshot["indexes"]["count"]),
            "canonical_sha256": _canonical_sha256(index_objects),
        },
        "schema_version": int(snapshot["schema_version"]),
        "user_version": int(snapshot["user_version"]),
        "core_table_counts": counts,
        "quick_check": dict(snapshot["quick_check"]),
        "foreign_key_check": dict(snapshot["foreign_key_check"]),
    }


def _all_application_tables(database_path: Path) -> tuple[str, ...]:
    try:
        snapshot = inspect_sqlite_database(database_path, ())
    except BaselineError as exc:
        raise BackupError("unable to enumerate production tables") from exc
    tables = tuple(
        sorted(
            str(item["name"])
            for item in snapshot["schema"]["objects"]
            if item["type"] == "table"
        )
    )
    return _deduplicate_core_tables(tables)


def _temporary_path(parent: Path, final_name: str) -> Path:
    descriptor, raw_path = tempfile.mkstemp(prefix=f".{final_name}.", suffix=".tmp", dir=parent)
    os.close(descriptor)
    return Path(raw_path)


def _fsync_file(path: Path) -> None:
    # Windows rejects fsync on a descriptor opened read-only. The backup is
    # already closed here, so open a dedicated read/write descriptor solely
    # to force its bytes to durable storage.
    descriptor = os.open(path, os.O_RDWR)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(directory: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_replace(source: Path, destination: Path) -> None:
    os.replace(source, destination)


def _atomic_publish_new(source: Path, destination: Path) -> None:
    """Publish *source* without ever replacing an existing destination."""

    # source and destination are deliberately in the same directory. Creating
    # a hard link is atomic and, unlike os.replace(), fails if destination was
    # created after our preflight check. The temporary name is best-effort
    # unlinked here and again by the caller's finally block.
    os.link(source, destination, follow_symlinks=False)
    _unlink_owned_file(source)


def _unlink_owned_file(path: Path) -> None:
    try:
        if _path_exists(path) and not path.is_dir():
            path.unlink()
    except OSError:
        # Cleanup is best effort. A leftover file has no valid manifest and is
        # therefore rejected by verification and restore.
        pass


def _sqlite_online_copy(source_path: Path, destination_path: Path) -> None:
    uri = f"{source_path.resolve().as_uri()}?mode=ro"
    source: sqlite3.Connection | None = None
    destination: sqlite3.Connection | None = None
    try:
        source = sqlite3.connect(uri, uri=True, timeout=30.0)
        source.execute("PRAGMA query_only=ON")
        destination = sqlite3.connect(destination_path, timeout=30.0)
        source.backup(destination, pages=256, sleep=0.01)
        destination.commit()
        journal_mode_row = destination.execute("PRAGMA journal_mode=DELETE").fetchone()
        if journal_mode_row is None or str(journal_mode_row[0]).lower() != "delete":
            raise BackupError("SQLite backup could not be normalized to a standalone file")
        destination.commit()
    except sqlite3.Error as exc:
        raise BackupError("SQLite online backup failed") from exc
    finally:
        if destination is not None:
            destination.close()
        if source is not None:
            source.close()
    _fsync_file(destination_path)


def _write_json_fsynced(path: Path, payload: dict[str, Any]) -> None:
    rendered = (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
    with path.open("wb") as handle:
        handle.write(rendered)
        handle.flush()
        os.fsync(handle.fileno())


def _default_manifest_path(backup_path: Path) -> Path:
    return backup_path.with_name(f"{backup_path.name}.manifest.json")


def _sqlite_path_families_overlap(left: Path, right: Path) -> bool:
    return left == right or left in _sidecar_paths(right) or right in _sidecar_paths(left)


def create_backup(
    database_path: Path,
    output_path: Path,
    *,
    manifest_path: Path | None = None,
    core_tables: Sequence[str] = DEFAULT_BACKUP_CORE_TABLES,
) -> dict[str, Any]:
    """Create and atomically publish a verified online backup and manifest."""

    _reject_symlink(database_path, "source database")
    _reject_symlink(output_path, "backup")
    database_path = database_path.resolve()
    output_path = output_path.resolve()
    unresolved_manifest_path = manifest_path or _default_manifest_path(output_path)
    _reject_symlink(unresolved_manifest_path, "manifest")
    manifest_path = unresolved_manifest_path.resolve()
    _require_regular_file(database_path, "source database")
    _require_existing_parent(output_path, "backup")
    _require_existing_parent(manifest_path, "manifest")
    if output_path.parent != manifest_path.parent:
        raise BackupError("backup and manifest must be published in the same directory")
    if (
        _sqlite_path_families_overlap(database_path, output_path)
        or _sqlite_path_families_overlap(database_path, manifest_path)
        or _sqlite_path_families_overlap(output_path, manifest_path)
    ):
        raise BackupError("source database, backup, and manifest SQLite path families must be distinct")
    if _path_exists(output_path) or _path_exists(manifest_path):
        raise BackupError("backup output or manifest already exists")

    required_tables = _deduplicate_core_tables(core_tables)
    backup_temp = _temporary_path(output_path.parent, output_path.name)
    manifest_temp = _temporary_path(manifest_path.parent, manifest_path.name)
    published_backup = False
    published_manifest = False
    try:
        _sqlite_online_copy(database_path, backup_temp)
        metadata = _database_metadata(backup_temp, required_tables)
        manifest = {
            "format_version": FORMAT_VERSION,
            "kind": MANIFEST_KIND,
            "created_at_utc": _utc_now(),
            "backup": {
                "name": output_path.name,
                "sha256": sha256_file(backup_temp),
                "size_bytes": backup_temp.stat().st_size,
            },
            "database": metadata,
        }
        manifest["manifest_sha256"] = _manifest_canonical_sha256(manifest)
        _write_json_fsynced(manifest_temp, manifest)

        _atomic_publish_new(backup_temp, output_path)
        published_backup = True
        _fsync_directory(output_path.parent)
        _atomic_publish_new(manifest_temp, manifest_path)
        published_manifest = True
        _fsync_directory(manifest_path.parent)
        return manifest
    except (OSError, BackupError) as exc:
        if published_manifest:
            _unlink_owned_file(manifest_path)
        if published_backup:
            _unlink_owned_file(output_path)
        if isinstance(exc, BackupError):
            raise
        raise BackupError("backup publication failed") from exc
    finally:
        _unlink_owned_file(backup_temp)
        _unlink_owned_file(manifest_temp)


def _load_manifest(manifest_path: Path) -> dict[str, Any]:
    _require_regular_file(manifest_path, "manifest")
    if manifest_path.stat().st_size > MAX_MANIFEST_BYTES:
        raise BackupError("manifest is too large")
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise BackupError("manifest is not valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise BackupError("manifest root must be an object")
    if payload.get("format_version") != FORMAT_VERSION or payload.get("kind") != MANIFEST_KIND:
        raise BackupError("manifest format is unsupported")
    if set(payload) != {
        "format_version",
        "kind",
        "created_at_utc",
        "backup",
        "database",
        "manifest_sha256",
    }:
        raise BackupError("manifest fields do not match the strict format")
    backup = payload.get("backup")
    database = payload.get("database")
    if not isinstance(backup, dict) or set(backup) != {"name", "sha256", "size_bytes"}:
        raise BackupError("manifest backup identity is invalid")
    if not isinstance(database, dict):
        raise BackupError("manifest database metadata is invalid")
    created_at = payload.get("created_at_utc")
    if not isinstance(created_at, str) or not created_at.endswith("Z"):
        raise BackupError("manifest creation timestamp is invalid")
    try:
        datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BackupError("manifest creation timestamp is invalid") from exc
    if (
        not isinstance(backup["name"], str)
        or not backup["name"]
        or Path(backup["name"]).name != backup["name"]
    ):
        raise BackupError("manifest backup name is invalid")
    if (
        not isinstance(backup["sha256"], str)
        or len(backup["sha256"]) != 64
        or any(character not in "0123456789abcdef" for character in backup["sha256"])
    ):
        raise BackupError("manifest backup hash is invalid")
    if type(backup["size_bytes"]) is not int or backup["size_bytes"] <= 0:
        raise BackupError("manifest backup size is invalid")
    manifest_sha256 = payload.get("manifest_sha256")
    if (
        not isinstance(manifest_sha256, str)
        or len(manifest_sha256) != 64
        or any(character not in "0123456789abcdef" for character in manifest_sha256)
        or manifest_sha256 != _manifest_canonical_sha256(payload)
    ):
        raise BackupError("manifest canonical hash does not match")
    return payload


def _verify_payload(backup_path: Path, manifest: dict[str, Any], *, require_name: bool) -> None:
    _reject_sidecars(backup_path, "backup")
    _require_regular_file(backup_path, "backup")
    expected_backup = manifest["backup"]
    if require_name and backup_path.name != expected_backup["name"]:
        raise BackupError("backup filename does not match manifest")
    if backup_path.stat().st_size != expected_backup["size_bytes"]:
        raise BackupError("backup size does not match manifest")
    if sha256_file(backup_path) != expected_backup["sha256"]:
        raise BackupError("backup hash does not match manifest")

    expected_database = manifest["database"]
    counts = expected_database.get("core_table_counts")
    if not isinstance(counts, dict):
        raise BackupError("manifest core table counts are invalid")
    core_tables = _deduplicate_core_tables(tuple(counts))
    actual_database = _database_metadata(backup_path, core_tables)
    _reject_sidecars(backup_path, "backup")
    if actual_database != expected_database:
        raise BackupError("backup database metadata does not match manifest")


def verify_backup(backup_path: Path, manifest_path: Path | None = None) -> dict[str, Any]:
    """Strictly verify bytes, schema, indexes, migrations, counts, and integrity."""

    _reject_symlink(backup_path, "backup")
    backup_path = backup_path.resolve()
    unresolved_manifest_path = manifest_path or _default_manifest_path(backup_path)
    _reject_symlink(unresolved_manifest_path, "manifest")
    manifest_path = unresolved_manifest_path.resolve()
    if _sqlite_path_families_overlap(backup_path, manifest_path):
        raise BackupError("backup and manifest SQLite path families must be distinct")
    manifest = _load_manifest(manifest_path)
    _verify_payload(backup_path, manifest, require_name=True)
    return manifest


def _copy_file_fsynced(source: Path, destination: Path) -> None:
    with source.open("rb") as source_handle, destination.open("wb") as destination_handle:
        shutil.copyfileobj(source_handle, destination_handle, length=1024 * 1024)
        destination_handle.flush()
        os.fsync(destination_handle.fileno())


def _sidecar_paths(database_path: Path) -> tuple[Path, ...]:
    return tuple(database_path.with_name(f"{database_path.name}{suffix}") for suffix in SQLITE_SIDECAR_SUFFIXES)


def _reject_sidecars(database_path: Path, label: str = "target") -> None:
    if any(_path_exists(path) for path in _sidecar_paths(database_path)):
        raise BackupError(f"{label} SQLite sidecars must be absent")


def _same_filesystem(left: Path, right: Path) -> bool:
    return os.stat(left).st_dev == os.stat(right).st_dev


def restore_backup(
    backup_path: Path,
    target_path: Path,
    *,
    manifest_path: Path | None = None,
    replace_production: bool = False,
    services_stopped: bool = False,
) -> dict[str, Any]:
    """Restore to an isolated target, or explicitly replace a stopped production DB."""

    original_target = target_path
    _reject_symlink(target_path, "restore target")
    manifest = verify_backup(backup_path, manifest_path)
    backup_path = backup_path.resolve()
    manifest_path = (manifest_path or _default_manifest_path(backup_path)).resolve()
    target_path = target_path.resolve()
    _require_existing_parent(target_path, "restore target")
    if _sqlite_path_families_overlap(target_path, backup_path) or target_path == manifest_path:
        raise BackupError("restore target must differ from backup and its SQLite sidecars")
    _reject_sidecars(target_path)

    target_exists = _path_exists(target_path)
    if not replace_production:
        if target_exists:
            raise BackupError("isolated restore target already exists")
    else:
        if not original_target.is_absolute():
            raise BackupError("production replace requires an absolute target path")
        if not services_stopped:
            raise BackupError("production replace requires an explicit services-stopped declaration")
        if not target_exists:
            raise BackupError("production replace target does not exist")
        _require_regular_file(target_path, "production target")
        if not _same_filesystem(backup_path, target_path.parent):
            raise BackupError("production backup and target must be on the same filesystem")

    restore_temp = _temporary_path(target_path.parent, target_path.name)
    rollback_path: Path | None = None
    published_target = False
    try:
        _copy_file_fsynced(backup_path, restore_temp)
        _verify_payload(restore_temp, manifest, require_name=False)

        if replace_production:
            rollback_path = target_path.with_name(
                f"{target_path.name}.pre-restore-{_filename_timestamp()}.sqlite"
            )
            if _path_exists(rollback_path):
                raise BackupError("production rollback backup already exists")
            create_backup(
                target_path,
                rollback_path,
                core_tables=_all_application_tables(target_path),
            )

        if replace_production:
            _atomic_replace(restore_temp, target_path)
        else:
            _atomic_publish_new(restore_temp, target_path)
        published_target = True
        _fsync_directory(target_path.parent)
        return {
            "status": "restored",
            "target": target_path.name,
            "rollback_backup": rollback_path.name if rollback_path else None,
            "rollback_manifest": (
                _default_manifest_path(rollback_path).name if rollback_path else None
            ),
            "sha256": manifest["backup"]["sha256"],
        }
    except (OSError, BackupError) as exc:
        if published_target:
            if rollback_path is None:
                _unlink_owned_file(target_path)
            else:
                recovery_temp = _temporary_path(target_path.parent, target_path.name)
                try:
                    rollback_manifest_path = _default_manifest_path(rollback_path)
                    rollback_manifest = verify_backup(rollback_path, rollback_manifest_path)
                    _copy_file_fsynced(rollback_path, recovery_temp)
                    _verify_payload(recovery_temp, rollback_manifest, require_name=False)
                    _atomic_replace(recovery_temp, target_path)
                    _fsync_directory(target_path.parent)
                except (OSError, BackupError) as rollback_exc:
                    raise BackupError("restore failed and automatic production rollback failed") from rollback_exc
                finally:
                    _unlink_owned_file(recovery_temp)
        if isinstance(exc, BackupError):
            raise
        raise BackupError("restore publication failed") from exc
    finally:
        _unlink_owned_file(restore_temp)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    backup_parser = subparsers.add_parser("backup", help="create an online backup")
    backup_parser.add_argument("--database", required=True, type=Path)
    backup_parser.add_argument("--output", required=True, type=Path)
    backup_parser.add_argument("--manifest", type=Path)
    backup_parser.add_argument("--core-table", action="append", default=[])

    verify_parser = subparsers.add_parser("verify", help="strictly verify a backup")
    verify_parser.add_argument("--backup", required=True, type=Path)
    verify_parser.add_argument("--manifest", type=Path)

    restore_parser = subparsers.add_parser("restore", help="restore a verified backup")
    restore_parser.add_argument("--backup", required=True, type=Path)
    restore_parser.add_argument("--manifest", type=Path)
    restore_parser.add_argument("--target", required=True, type=Path)
    restore_parser.add_argument("--replace-production", action="store_true")
    restore_parser.add_argument(
        "--services-stopped",
        action="store_true",
        help="explicit declaration that every process using the production database is stopped",
    )
    return parser


def _print_result(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "backup":
            manifest = create_backup(
                args.database,
                args.output,
                manifest_path=args.manifest,
                core_tables=args.core_table or DEFAULT_BACKUP_CORE_TABLES,
            )
            _print_result(
                {
                    "status": "created",
                    "backup": manifest["backup"]["name"],
                    "sha256": manifest["backup"]["sha256"],
                    "manifest_sha256": manifest["manifest_sha256"],
                }
            )
        elif args.command == "verify":
            manifest = verify_backup(args.backup, args.manifest)
            _print_result(
                {
                    "status": "verified",
                    "backup": manifest["backup"]["name"],
                    "sha256": manifest["backup"]["sha256"],
                    "manifest_sha256": manifest["manifest_sha256"],
                }
            )
        else:
            _print_result(
                restore_backup(
                    args.backup,
                    args.target,
                    manifest_path=args.manifest,
                    replace_production=args.replace_production,
                    services_stopped=args.services_stopped,
                )
            )
        return 0
    except BackupError as exc:
        print(f"sqlite backup command failed: {exc}", file=sys.stderr)
        return 1
    except OSError:
        # OSError messages may contain absolute host paths. Keep CLI output
        # secret-safe and path-independent, consistent with the PR0 baseline.
        print("sqlite backup command failed: operating system error", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
