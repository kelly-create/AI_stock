#!/usr/bin/env python3
"""Create, verify, and safely restore a SQLite-bound research raw archive."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import sqlite3
import stat
import sys
import tarfile
import tempfile
import time
from typing import Any, BinaryIO, Sequence


FORMAT_VERSION = 1
MANIFEST_KIND = "dsa-research-raw-archive"
ARCHIVE_FORMAT = "tar"
MAX_MANIFEST_BYTES = 64 * 1024 * 1024
SQLITE_SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")
_SHA256_HEX = frozenset("0123456789abcdef")


class RawArchiveError(RuntimeError):
    """Raised when the raw archive contract cannot be completed safely."""


@dataclass(frozen=True, order=True)
class ArtifactIdentity:
    relative_path: str
    content_sha256: str
    compressed_sha256: str
    compressed_size_bytes: int
    uncompressed_size_bytes: int


@dataclass(frozen=True, order=True)
class DatabaseReference:
    relative_path: str
    content_sha256: str
    compressed_bytes: int | None
    uncompressed_bytes: int | None


@dataclass(frozen=True)
class RawFileEntry:
    relative_path: str
    source: Path
    size_bytes: int
    mtime_ns: int
    device: int
    inode: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _path_exists(path: Path) -> bool:
    return os.path.lexists(path)


def _is_link_like(path: Path) -> bool:
    if path.is_symlink():
        return True
    is_junction = getattr(path, "is_junction", None)
    return bool(is_junction and is_junction())


def _absolute_path(path: str | Path) -> Path:
    return Path(os.path.abspath(os.path.expanduser(os.fspath(path))))


def _reject_link_components(path: Path, label: str) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:]:
        current = current / part
        if _path_exists(current) and _is_link_like(current):
            raise RawArchiveError(f"{label} must not contain symbolic links or junctions")


def _require_regular_file(path: Path, label: str) -> None:
    _reject_link_components(path, label)
    try:
        mode = path.stat(follow_symlinks=False).st_mode
    except OSError as exc:
        raise RawArchiveError(f"{label} must be an existing regular file") from exc
    if not stat.S_ISREG(mode):
        raise RawArchiveError(f"{label} must be an existing regular file")


def _require_directory(path: Path, label: str) -> None:
    _reject_link_components(path, label)
    try:
        mode = path.stat(follow_symlinks=False).st_mode
    except OSError as exc:
        raise RawArchiveError(f"{label} must be an existing directory") from exc
    if not stat.S_ISDIR(mode):
        raise RawArchiveError(f"{label} must be an existing directory")


def _require_existing_parent(path: Path, label: str) -> None:
    _require_directory(path.parent, f"{label} parent")


def _same_path(left: Path, right: Path) -> bool:
    return os.path.normcase(os.fspath(left)) == os.path.normcase(os.fspath(right))


def _is_within(path: Path, root: Path) -> bool:
    try:
        common = os.path.commonpath((os.fspath(path), os.fspath(root)))
    except ValueError:
        return False
    return os.path.normcase(common) == os.path.normcase(os.fspath(root))


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and set(value) <= _SHA256_HEX


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(value: Any) -> str:
    rendered = json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(rendered).hexdigest()


def _manifest_canonical_sha256(manifest: dict[str, Any]) -> str:
    return _canonical_sha256({key: value for key, value in manifest.items() if key != "manifest_sha256"})


def _fsync_file(path: Path) -> None:
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


def _temporary_file(parent: Path, final_name: str) -> Path:
    descriptor, raw_path = tempfile.mkstemp(prefix=f".{final_name}.", suffix=".tmp", dir=parent)
    os.close(descriptor)
    return Path(raw_path)


def _unlink_owned_file(path: Path) -> None:
    try:
        if _path_exists(path) and not path.is_dir():
            path.unlink()
    except OSError:
        pass


def _atomic_publish_new(source: Path, destination: Path) -> None:
    """Atomically publish a file while refusing a concurrently created target."""

    os.link(source, destination, follow_symlinks=False)
    # Publication succeeded once the hard link exists. Temporary-name cleanup
    # must not turn that success into an exception before the caller records
    # ownership of the published destination.
    _unlink_owned_file(source)


def _write_json_fsynced(path: Path, payload: dict[str, Any]) -> None:
    rendered = (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
    with path.open("wb") as handle:
        handle.write(rendered)
        handle.flush()
        os.fsync(handle.fileno())


def _default_manifest_path(archive_path: Path) -> Path:
    return archive_path.with_name(f"{archive_path.name}.manifest.json")


def _parse_content_path(value: Any) -> tuple[str, str]:
    if not isinstance(value, str) or not value or "\\" in value:
        raise RawArchiveError("raw path must be a non-empty POSIX relative path")
    pure = PurePosixPath(value)
    if pure.is_absolute() or len(pure.parts) != 2 or any(part in {"", ".", ".."} for part in pure.parts):
        raise RawArchiveError("raw path is not a canonical content-addressed path")
    prefix, filename = pure.parts
    if len(prefix) != 2 or set(prefix) - _SHA256_HEX:
        raise RawArchiveError("raw path shard is not a lowercase SHA-256 prefix")
    if not filename.endswith(".gz"):
        raise RawArchiveError("raw path must end in .gz")
    stem = filename[:-3]
    pieces = stem.split(".")
    if len(pieces) != 2:
        raise RawArchiveError("raw filename is not canonical")
    digest, extension = pieces
    if not _is_sha256(digest) or digest[:2] != prefix:
        raise RawArchiveError("raw filename does not match its SHA-256 shard")
    if not extension or any(character not in "abcdefghijklmnopqrstuvwxyz0123456789_" for character in extension):
        raise RawArchiveError("raw filename extension is not canonical")
    return digest, value


def _enumerate_raw_root(raw_root: Path) -> tuple[RawFileEntry, ...]:
    """Enumerate the complete canonical tree without following links."""

    _require_directory(raw_root, "raw root")
    if raw_root == Path(raw_root.anchor):
        raise RawArchiveError("raw root must not be a filesystem root")
    entries: list[RawFileEntry] = []
    try:
        root_entries = sorted(os.scandir(raw_root), key=lambda item: item.name)
        for shard in root_entries:
            shard_path = raw_root / shard.name
            if shard.is_symlink() or _is_link_like(shard_path):
                raise RawArchiveError("raw root contains a symbolic link or junction")
            if len(shard.name) != 2 or set(shard.name) - _SHA256_HEX:
                raise RawArchiveError("raw root contains a noncanonical staging, temp, or other entry")
            if not shard.is_dir(follow_symlinks=False):
                raise RawArchiveError("raw root shard must be a regular directory")
            for candidate in sorted(os.scandir(shard_path), key=lambda item: item.name):
                candidate_path = shard_path / candidate.name
                if candidate.is_symlink() or _is_link_like(candidate_path):
                    raise RawArchiveError("raw root contains a symbolic link or junction")
                if not candidate.is_file(follow_symlinks=False):
                    raise RawArchiveError("raw root contains a noncanonical nested entry")
                relative_path = f"{shard.name}/{candidate.name}"
                _parse_content_path(relative_path)
                # ``DirEntry.stat()`` reports zero ``st_dev``/``st_ino`` on
                # some Windows Python builds while ``Path.stat()`` exposes
                # the real file identity. Use the same API here and during
                # the publication recheck so immutable files compare equally.
                metadata = candidate_path.stat(follow_symlinks=False)
                entries.append(
                    RawFileEntry(
                        relative_path=relative_path,
                        source=candidate_path,
                        size_bytes=int(metadata.st_size),
                        mtime_ns=int(metadata.st_mtime_ns),
                        device=int(metadata.st_dev),
                        inode=int(metadata.st_ino),
                    )
                )
    except OSError as exc:
        raise RawArchiveError("raw root could not be enumerated safely") from exc
    return tuple(entries)


def _write_tar_archive(path: Path, entries: Sequence[RawFileEntry]) -> None:
    try:
        # GNU tar keeps long, but still canonical, RawArtifactStore extensions
        # representable without adding PAX headers whose metadata may vary
        # across Python versions.
        with tarfile.open(path, mode="w:", format=tarfile.GNU_FORMAT) as archive:
            for entry in entries:
                current = entry.source.stat(follow_symlinks=False)
                if not stat.S_ISREG(current.st_mode):
                    raise RawArchiveError("raw artifact changed type during archive creation")
                if (
                    int(current.st_size),
                    int(current.st_mtime_ns),
                    int(current.st_dev),
                    int(current.st_ino),
                ) != (entry.size_bytes, entry.mtime_ns, entry.device, entry.inode):
                    raise RawArchiveError("raw artifact changed during archive creation")
                info = tarfile.TarInfo(entry.relative_path)
                info.size = entry.size_bytes
                info.mtime = 0
                info.mode = 0o600
                info.uid = 0
                info.gid = 0
                info.uname = ""
                info.gname = ""
                with entry.source.open("rb") as source:
                    archive.addfile(info, source)
        _fsync_file(path)
    except (OSError, ValueError, tarfile.TarError) as exc:
        raise RawArchiveError("raw tar archive could not be created") from exc


class _HashingReader:
    def __init__(self, raw: BinaryIO) -> None:
        self.raw = raw
        self.digest = hashlib.sha256()
        self.bytes_read = 0

    def read(self, size: int = -1) -> bytes:
        data = self.raw.read(size)
        self.digest.update(data)
        self.bytes_read += len(data)
        return data


def _validate_member_metadata(member: tarfile.TarInfo) -> tuple[str, str]:
    digest, relative_path = _parse_content_path(member.name)
    if (
        member.type != tarfile.REGTYPE
        or member.linkname
        or member.pax_headers
        or member.mtime != 0
        or member.mode != 0o600
        or member.uid != 0
        or member.gid != 0
        or member.uname
        or member.gname
        or member.size <= 0
    ):
        raise RawArchiveError("archive member metadata does not match the strict format")
    return digest, relative_path


def _inspect_member(archive: tarfile.TarFile, member: tarfile.TarInfo) -> ArtifactIdentity:
    digest, relative_path = _validate_member_metadata(member)
    extracted = archive.extractfile(member)
    if extracted is None:
        raise RawArchiveError("archive member is not readable")
    reader = _HashingReader(extracted)
    content_digest = hashlib.sha256()
    uncompressed_size = 0
    try:
        with gzip.GzipFile(fileobj=reader, mode="rb") as payload:
            for chunk in iter(lambda: payload.read(1024 * 1024), b""):
                content_digest.update(chunk)
                uncompressed_size += len(chunk)
        for _chunk in iter(lambda: reader.read(1024 * 1024), b""):
            pass
    except (EOFError, OSError) as exc:
        raise RawArchiveError("archive member is not valid gzip data") from exc
    finally:
        extracted.close()
    if reader.bytes_read != member.size:
        raise RawArchiveError("archive member compressed size is inconsistent")
    if content_digest.hexdigest() != digest:
        raise RawArchiveError("archive member content does not match its filename SHA-256")
    return ArtifactIdentity(
        relative_path=relative_path,
        content_sha256=digest,
        compressed_sha256=reader.digest.hexdigest(),
        compressed_size_bytes=int(member.size),
        uncompressed_size_bytes=uncompressed_size,
    )


def _inspect_archive(archive_path: Path) -> tuple[ArtifactIdentity, ...]:
    artifacts: list[ArtifactIdentity] = []
    names: set[str] = set()
    try:
        with tarfile.open(archive_path, mode="r:") as archive:
            members = archive.getmembers()
            if [member.name for member in members] != sorted(member.name for member in members):
                raise RawArchiveError("archive member order is not canonical")
            for member in members:
                if member.name in names:
                    raise RawArchiveError("archive contains duplicate member paths")
                names.add(member.name)
                artifacts.append(_inspect_member(archive, member))
    except (OSError, tarfile.TarError) as exc:
        raise RawArchiveError("archive is not a readable uncompressed tar file") from exc
    return tuple(sorted(artifacts))


def _reject_database_sidecars(database_path: Path) -> None:
    for suffix in SQLITE_SIDECAR_SUFFIXES:
        sidecar = database_path.with_name(f"{database_path.name}{suffix}")
        if _path_exists(sidecar):
            raise RawArchiveError("SQLite backup sidecars must be absent")


def _parse_database_reference(raw_value: Any) -> DatabaseReference:
    if not isinstance(raw_value, str) or not raw_value:
        raise RawArchiveError("research raw reference is not valid JSON text")
    try:
        value = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise RawArchiveError("research raw reference is not valid JSON text") from exc
    if not isinstance(value, dict):
        raise RawArchiveError("research raw reference must be a JSON object")
    digest = str(value.get("content_sha256") or "").strip().lower()
    if not _is_sha256(digest):
        raise RawArchiveError("research raw reference has an invalid content SHA-256")
    parsed_digest, relative_path = _parse_content_path(value.get("relative_path"))
    if digest != parsed_digest:
        raise RawArchiveError("research raw reference path does not match its content SHA-256")
    if str(value.get("compression") or "").strip().lower() != "gzip":
        raise RawArchiveError("research raw reference compression must be gzip")

    def optional_size(field: str) -> int | None:
        raw_size = value.get(field)
        if raw_size is None:
            return None
        if type(raw_size) is not int or raw_size < 0:
            raise RawArchiveError(f"research raw reference {field} is invalid")
        return raw_size

    return DatabaseReference(
        relative_path=relative_path,
        content_sha256=digest,
        compressed_bytes=optional_size("compressed_bytes"),
        uncompressed_bytes=optional_size("uncompressed_bytes"),
    )


def _read_database_references(database_path: Path) -> tuple[DatabaseReference, ...]:
    _require_regular_file(database_path, "SQLite backup")
    _reject_database_sidecars(database_path)
    uri = f"{database_path.as_uri()}?mode=ro&immutable=1"
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=5.0)
        connection.execute("PRAGMA query_only=ON")
        quick_check = [str(row[0]) for row in connection.execute("PRAGMA quick_check").fetchall()]
        if quick_check != ["ok"]:
            raise RawArchiveError("SQLite backup quick_check failed")
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'research_dataset_snapshots'"
        ).fetchone()
        if table is None:
            raise RawArchiveError("SQLite backup is missing research_dataset_snapshots")
        columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(research_dataset_snapshots)").fetchall()
        }
        if "raw_ref_json" not in columns:
            raise RawArchiveError("SQLite backup is missing research_dataset_snapshots.raw_ref_json")
        rows = connection.execute(
            "SELECT raw_ref_json FROM research_dataset_snapshots "
            "WHERE raw_ref_json IS NOT NULL AND raw_ref_json <> ''"
        ).fetchall()
        return tuple(_parse_database_reference(row[0]) for row in rows)
    except sqlite3.Error as exc:
        raise RawArchiveError("SQLite backup could not be read safely") from exc
    finally:
        if connection is not None:
            connection.close()


def _database_identity_and_references(
    database_path: Path,
) -> tuple[dict[str, Any], tuple[DatabaseReference, ...]]:
    size_before = database_path.stat().st_size
    hash_before = _sha256_file(database_path)
    references = _read_database_references(database_path)
    size_after = database_path.stat().st_size
    hash_after = _sha256_file(database_path)
    if size_before != size_after or hash_before != hash_after:
        raise RawArchiveError("SQLite backup changed while raw references were being read")
    return (
        {
            "name": database_path.name,
            "sha256": hash_after,
            "size_bytes": int(size_after),
        },
        references,
    )


def _reference_manifest(references: Sequence[DatabaseReference]) -> dict[str, Any]:
    unique = sorted({(reference.relative_path, reference.content_sha256) for reference in references})
    return {
        "raw_reference_rows": len(references),
        "artifact_count": len(unique),
        "artifacts": [
            {"relative_path": relative_path, "content_sha256": digest}
            for relative_path, digest in unique
        ],
    }


def _validate_references(
    references: Sequence[DatabaseReference],
    artifacts: Sequence[ArtifactIdentity],
) -> None:
    by_path = {artifact.relative_path: artifact for artifact in artifacts}
    for reference in references:
        artifact = by_path.get(reference.relative_path)
        if artifact is None:
            raise RawArchiveError("SQLite backup references a raw artifact missing from the archive")
        if artifact.content_sha256 != reference.content_sha256:
            raise RawArchiveError("SQLite backup raw reference does not match the archived artifact")
        if reference.compressed_bytes is not None and reference.compressed_bytes != artifact.compressed_size_bytes:
            raise RawArchiveError("SQLite backup raw reference compressed size does not match the archive")
        if (
            reference.uncompressed_bytes is not None
            and reference.uncompressed_bytes != artifact.uncompressed_size_bytes
        ):
            raise RawArchiveError("SQLite backup raw reference uncompressed size does not match the archive")


def _validate_file_identity(value: Any, label: str, *, archive: bool = False) -> dict[str, Any]:
    required = {"name", "sha256", "size_bytes"}
    if archive:
        required.add("format")
    if not isinstance(value, dict) or set(value) != required:
        raise RawArchiveError(f"manifest {label} identity is invalid")
    name = value.get("name")
    if (
        not isinstance(name, str)
        or not name
        or Path(name).name != name
        or "/" in name
        or "\\" in name
    ):
        raise RawArchiveError(f"manifest {label} name is invalid")
    if not _is_sha256(value.get("sha256")):
        raise RawArchiveError(f"manifest {label} SHA-256 is invalid")
    if type(value.get("size_bytes")) is not int or value["size_bytes"] <= 0:
        raise RawArchiveError(f"manifest {label} size is invalid")
    if archive and value.get("format") != ARCHIVE_FORMAT:
        raise RawArchiveError("manifest archive format is unsupported")
    return value


def _validate_artifact_manifest(value: Any) -> tuple[ArtifactIdentity, ...]:
    if not isinstance(value, list):
        raise RawArchiveError("manifest artifacts must be a list")
    artifacts: list[ArtifactIdentity] = []
    for item in value:
        if not isinstance(item, dict) or set(item) != {
            "relative_path",
            "content_sha256",
            "compressed_sha256",
            "compressed_size_bytes",
            "uncompressed_size_bytes",
        }:
            raise RawArchiveError("manifest artifact entry is invalid")
        digest, relative_path = _parse_content_path(item.get("relative_path"))
        if item.get("content_sha256") != digest or not _is_sha256(item.get("compressed_sha256")):
            raise RawArchiveError("manifest artifact hash is invalid")
        if type(item.get("compressed_size_bytes")) is not int or item["compressed_size_bytes"] <= 0:
            raise RawArchiveError("manifest artifact compressed size is invalid")
        if type(item.get("uncompressed_size_bytes")) is not int or item["uncompressed_size_bytes"] < 0:
            raise RawArchiveError("manifest artifact uncompressed size is invalid")
        artifacts.append(ArtifactIdentity(**item))
    if artifacts != sorted(artifacts) or len({item.relative_path for item in artifacts}) != len(artifacts):
        raise RawArchiveError("manifest artifact list is not canonical")
    return tuple(artifacts)


def _validate_reference_manifest(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"raw_reference_rows", "artifact_count", "artifacts"}:
        raise RawArchiveError("manifest references are invalid")
    if type(value.get("raw_reference_rows")) is not int or value["raw_reference_rows"] < 0:
        raise RawArchiveError("manifest raw reference row count is invalid")
    if type(value.get("artifact_count")) is not int or value["artifact_count"] < 0:
        raise RawArchiveError("manifest referenced artifact count is invalid")
    artifacts = value.get("artifacts")
    if not isinstance(artifacts, list):
        raise RawArchiveError("manifest referenced artifacts must be a list")
    normalized: list[dict[str, str]] = []
    for item in artifacts:
        if not isinstance(item, dict) or set(item) != {"relative_path", "content_sha256"}:
            raise RawArchiveError("manifest referenced artifact entry is invalid")
        digest, relative_path = _parse_content_path(item.get("relative_path"))
        if item.get("content_sha256") != digest:
            raise RawArchiveError("manifest referenced artifact hash is invalid")
        normalized.append({"relative_path": relative_path, "content_sha256": digest})
    expected = sorted(normalized, key=lambda item: (item["relative_path"], item["content_sha256"]))
    if normalized != expected or len({item["relative_path"] for item in normalized}) != len(normalized):
        raise RawArchiveError("manifest referenced artifact list is not canonical")
    if value["artifact_count"] != len(normalized):
        raise RawArchiveError("manifest referenced artifact count does not match its list")
    return value


def _load_manifest(manifest_path: Path) -> dict[str, Any]:
    _require_regular_file(manifest_path, "manifest")
    if manifest_path.stat().st_size > MAX_MANIFEST_BYTES:
        raise RawArchiveError("manifest is too large")
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RawArchiveError("manifest is not valid UTF-8 JSON") from exc
    if not isinstance(payload, dict) or set(payload) != {
        "format_version",
        "kind",
        "created_at_utc",
        "archive",
        "database_backup",
        "artifacts",
        "references",
        "manifest_sha256",
    }:
        raise RawArchiveError("manifest fields do not match the strict format")
    if payload.get("format_version") != FORMAT_VERSION or payload.get("kind") != MANIFEST_KIND:
        raise RawArchiveError("manifest format is unsupported")
    created_at = payload.get("created_at_utc")
    if not isinstance(created_at, str) or not created_at.endswith("Z"):
        raise RawArchiveError("manifest creation timestamp is invalid")
    try:
        datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RawArchiveError("manifest creation timestamp is invalid") from exc
    _validate_file_identity(payload.get("archive"), "archive", archive=True)
    _validate_file_identity(payload.get("database_backup"), "database backup")
    artifacts = _validate_artifact_manifest(payload.get("artifacts"))
    references = _validate_reference_manifest(payload.get("references"))
    artifact_paths = {artifact.relative_path for artifact in artifacts}
    if any(item["relative_path"] not in artifact_paths for item in references["artifacts"]):
        raise RawArchiveError("manifest references an artifact absent from its file list")
    manifest_hash = payload.get("manifest_sha256")
    if not _is_sha256(manifest_hash) or manifest_hash != _manifest_canonical_sha256(payload):
        raise RawArchiveError("manifest canonical hash does not match")
    return payload


def _prepare_inputs(
    archive_path: str | Path,
    database_backup: str | Path,
    manifest_path: str | Path | None,
) -> tuple[Path, Path, Path]:
    archive = _absolute_path(archive_path)
    database = _absolute_path(database_backup)
    manifest = _absolute_path(manifest_path) if manifest_path is not None else _default_manifest_path(archive)
    if _same_path(archive, database) or _same_path(archive, manifest) or _same_path(database, manifest):
        raise RawArchiveError("archive, manifest, and SQLite backup paths must be distinct")
    return archive, database, manifest


def create_raw_archive(
    raw_root: str | Path,
    database_backup: str | Path,
    archive_path: str | Path,
    *,
    manifest_path: str | Path | None = None,
) -> dict[str, Any]:
    """Create and publish a verified full raw archive bound to one SQLite backup."""

    root = _absolute_path(raw_root)
    archive, database, manifest = _prepare_inputs(archive_path, database_backup, manifest_path)
    _require_directory(root, "raw root")
    _require_regular_file(database, "SQLite backup")
    _require_existing_parent(archive, "archive")
    _require_existing_parent(manifest, "manifest")
    if not _same_path(archive.parent, manifest.parent):
        raise RawArchiveError("archive and manifest must be published in the same directory")
    if _is_within(archive, root) or _is_within(manifest, root) or _is_within(database, root):
        raise RawArchiveError("archive, manifest, and SQLite backup must be outside the raw root")
    if _path_exists(archive) or _path_exists(manifest):
        raise RawArchiveError("archive output or manifest already exists")

    initial_entries = _enumerate_raw_root(root)
    archive_temp = _temporary_file(archive.parent, archive.name)
    manifest_temp = _temporary_file(manifest.parent, manifest.name)
    published_archive = False
    published_manifest = False
    try:
        _write_tar_archive(archive_temp, initial_entries)
        artifacts = _inspect_archive(archive_temp)
        database_identity, references = _database_identity_and_references(database)
        _validate_references(references, artifacts)
        manifest_payload: dict[str, Any] = {
            "format_version": FORMAT_VERSION,
            "kind": MANIFEST_KIND,
            "created_at_utc": _utc_now(),
            "archive": {
                "name": archive.name,
                "format": ARCHIVE_FORMAT,
                "sha256": _sha256_file(archive_temp),
                "size_bytes": archive_temp.stat().st_size,
            },
            "database_backup": database_identity,
            "artifacts": [asdict(artifact) for artifact in artifacts],
            "references": _reference_manifest(references),
        }
        manifest_payload["manifest_sha256"] = _manifest_canonical_sha256(manifest_payload)
        _write_json_fsynced(manifest_temp, manifest_payload)
        if manifest_temp.stat().st_size > MAX_MANIFEST_BYTES:
            raise RawArchiveError("manifest is too large")
        if _load_manifest(manifest_temp) != manifest_payload:
            raise RawArchiveError("generated manifest failed strict validation")

        if _enumerate_raw_root(root) != initial_entries:
            raise RawArchiveError("raw root changed during archive creation")
        if database.stat().st_size != database_identity["size_bytes"] or _sha256_file(database) != database_identity["sha256"]:
            raise RawArchiveError("SQLite backup changed during archive creation")

        _atomic_publish_new(archive_temp, archive)
        published_archive = True
        _fsync_directory(archive.parent)
        _atomic_publish_new(manifest_temp, manifest)
        published_manifest = True
        _fsync_directory(manifest.parent)
        return manifest_payload
    except (OSError, RawArchiveError) as exc:
        if published_manifest:
            _unlink_owned_file(manifest)
        if published_archive:
            _unlink_owned_file(archive)
        if isinstance(exc, RawArchiveError):
            raise
        raise RawArchiveError("raw archive publication failed") from exc
    finally:
        _unlink_owned_file(archive_temp)
        _unlink_owned_file(manifest_temp)


def verify_raw_archive(
    archive_path: str | Path,
    database_backup: str | Path,
    *,
    manifest_path: str | Path | None = None,
) -> dict[str, Any]:
    """Verify the archive, manifest, SQLite identity, references, and every gzip payload."""

    archive, database, manifest_path_value = _prepare_inputs(archive_path, database_backup, manifest_path)
    _require_regular_file(archive, "archive")
    _require_regular_file(database, "SQLite backup")
    payload = _load_manifest(manifest_path_value)
    if archive.name != payload["archive"]["name"]:
        raise RawArchiveError("archive filename does not match manifest")
    if archive.stat().st_size != payload["archive"]["size_bytes"]:
        raise RawArchiveError("archive size does not match manifest")
    if _sha256_file(archive) != payload["archive"]["sha256"]:
        raise RawArchiveError("archive SHA-256 does not match manifest")

    database_identity, references = _database_identity_and_references(database)
    expected_database = payload["database_backup"]
    if (
        database_identity["sha256"] != expected_database["sha256"]
        or database_identity["size_bytes"] != expected_database["size_bytes"]
    ):
        raise RawArchiveError("SQLite backup identity does not match manifest")
    if _reference_manifest(references) != payload["references"]:
        raise RawArchiveError("SQLite raw references do not match manifest")
    artifacts = _inspect_archive(archive)
    if [asdict(artifact) for artifact in artifacts] != payload["artifacts"]:
        raise RawArchiveError("archive artifact list, hashes, or sizes do not match manifest")
    _validate_references(references, artifacts)
    return payload


def _copy_member_fsynced(source: BinaryIO, destination: Path, expected: ArtifactIdentity) -> None:
    digest = hashlib.sha256()
    total = 0
    with destination.open("xb") as target:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            target.write(chunk)
            digest.update(chunk)
            total += len(chunk)
        target.flush()
        os.fsync(target.fileno())
    if total != expected.compressed_size_bytes or digest.hexdigest() != expected.compressed_sha256:
        raise RawArchiveError("restored raw artifact does not match the verified archive")


def _extract_to_staging(archive_path: Path, staging: Path, artifacts: Sequence[ArtifactIdentity]) -> None:
    expected_by_path = {artifact.relative_path: artifact for artifact in artifacts}
    created_directories: set[Path] = set()
    try:
        with tarfile.open(archive_path, mode="r:") as archive:
            members = archive.getmembers()
            if [member.name for member in members] != [artifact.relative_path for artifact in artifacts]:
                raise RawArchiveError("archive changed after verification")
            for member in members:
                expected = expected_by_path[member.name]
                _validate_member_metadata(member)
                if member.size != expected.compressed_size_bytes:
                    raise RawArchiveError("archive member changed after verification")
                shard = staging / PurePosixPath(member.name).parts[0]
                if shard not in created_directories:
                    shard.mkdir(mode=0o700)
                    created_directories.add(shard)
                destination = staging.joinpath(*PurePosixPath(member.name).parts)
                source = archive.extractfile(member)
                if source is None:
                    raise RawArchiveError("archive member is not readable during restore")
                try:
                    _copy_member_fsynced(source, destination, expected)
                finally:
                    source.close()
                os.chmod(destination, 0o600)
        for directory in sorted(created_directories):
            _fsync_directory(directory)
        _fsync_directory(staging)
    except (OSError, tarfile.TarError) as exc:
        raise RawArchiveError("raw archive could not be extracted safely") from exc


def _move_restore_shard(source: Path, destination: Path) -> None:
    if _path_exists(destination):
        raise RawArchiveError("isolated restore target changed during publication")
    os.rename(source, destination)


def _cleanup_tree(path: Path) -> None:
    if _path_exists(path):
        shutil.rmtree(path)


def restore_raw_archive(
    archive_path: str | Path,
    database_backup: str | Path,
    target_root: str | Path,
    *,
    manifest_path: str | Path | None = None,
) -> dict[str, Any]:
    """Restore only into a newly claimed isolated raw root; never replace an existing path."""

    payload = verify_raw_archive(archive_path, database_backup, manifest_path=manifest_path)
    archive, database, manifest = _prepare_inputs(archive_path, database_backup, manifest_path)
    target = _absolute_path(target_root)
    _require_existing_parent(target, "restore target")
    _reject_link_components(target, "restore target")
    if target == Path(target.anchor):
        raise RawArchiveError("restore target must not be a filesystem root")
    if any(_same_path(target, candidate) for candidate in (archive, database, manifest)):
        raise RawArchiveError("restore target must differ from archive inputs")
    if _path_exists(target):
        raise RawArchiveError("isolated restore target already exists")

    artifacts = tuple(ArtifactIdentity(**item) for item in payload["artifacts"])
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.restore-", dir=target.parent))
    claimed_target = False
    try:
        _extract_to_staging(archive, staging, artifacts)
        target.mkdir(mode=0o700, exist_ok=False)
        claimed_target = True
        for shard in sorted(staging.iterdir(), key=lambda path: path.name):
            _move_restore_shard(shard, target / shard.name)
        _fsync_directory(target)
        _fsync_directory(target.parent)
        staging.rmdir()
        return {
            "status": "restored",
            "target": target.name,
            "archive_sha256": payload["archive"]["sha256"],
            "database_sha256": payload["database_backup"]["sha256"],
            "artifact_count": len(artifacts),
        }
    except (OSError, RawArchiveError) as exc:
        cleanup_failed = False
        if claimed_target:
            try:
                _cleanup_tree(target)
            except OSError:
                cleanup_failed = True
        try:
            _cleanup_tree(staging)
        except OSError:
            cleanup_failed = True
        if cleanup_failed:
            raise RawArchiveError("raw restore failed and cleanup was incomplete") from exc
        if isinstance(exc, RawArchiveError):
            raise
        raise RawArchiveError("raw restore publication failed") from exc
    finally:
        if _path_exists(staging):
            try:
                _cleanup_tree(staging)
            except OSError:
                pass


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    create_parser = commands.add_parser("create", help="create a SQLite-bound full raw archive")
    create_parser.add_argument("--raw-root", required=True, type=Path)
    create_parser.add_argument("--database-backup", required=True, type=Path)
    create_parser.add_argument("--archive", required=True, type=Path)
    create_parser.add_argument("--manifest", type=Path)

    verify_parser = commands.add_parser("verify", help="strictly verify a full raw archive")
    verify_parser.add_argument("--archive", required=True, type=Path)
    verify_parser.add_argument("--database-backup", required=True, type=Path)
    verify_parser.add_argument("--manifest", type=Path)

    restore_parser = commands.add_parser("restore", help="restore into a new isolated raw root")
    restore_parser.add_argument("--archive", required=True, type=Path)
    restore_parser.add_argument("--database-backup", required=True, type=Path)
    restore_parser.add_argument("--manifest", type=Path)
    restore_parser.add_argument("--target-root", required=True, type=Path)
    return parser


def _print_result(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    started = time.monotonic()
    try:
        if args.command == "create":
            manifest = create_raw_archive(
                args.raw_root,
                args.database_backup,
                args.archive,
                manifest_path=args.manifest,
            )
            result = {
                "status": "created",
                "archive": manifest["archive"]["name"],
                "archive_sha256": manifest["archive"]["sha256"],
                "database_sha256": manifest["database_backup"]["sha256"],
                "manifest_sha256": manifest["manifest_sha256"],
                "artifact_count": len(manifest["artifacts"]),
                "referenced_artifact_count": manifest["references"]["artifact_count"],
            }
        elif args.command == "verify":
            manifest = verify_raw_archive(
                args.archive,
                args.database_backup,
                manifest_path=args.manifest,
            )
            result = {
                "status": "verified",
                "archive": manifest["archive"]["name"],
                "archive_sha256": manifest["archive"]["sha256"],
                "database_sha256": manifest["database_backup"]["sha256"],
                "manifest_sha256": manifest["manifest_sha256"],
                "artifact_count": len(manifest["artifacts"]),
                "referenced_artifact_count": manifest["references"]["artifact_count"],
            }
        else:
            result = restore_raw_archive(
                args.archive,
                args.database_backup,
                args.target_root,
                manifest_path=args.manifest,
            )
        result["elapsed_seconds"] = round(time.monotonic() - started, 6)
        _print_result(result)
        return 0
    except RawArchiveError as exc:
        print(f"research raw archive command failed: {exc}", file=sys.stderr)
        return 1
    except OSError:
        print("research raw archive command failed: operating system error", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
