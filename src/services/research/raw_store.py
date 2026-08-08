"""Content-addressed gzip storage for provider raw artifacts."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import gzip
import os
from pathlib import Path
import tempfile
import threading
from typing import Any

from .canonical import canonical_json, sha256_hex


# RawArtifactStore instances intentionally do not own these locks: callers may
# construct one store per job while publishing into the same content-addressed
# root. A bounded lock stripe keeps that cross-instance coordination process
# wide without retaining one lock forever for every artifact ever written.
_PUBLISH_LOCKS = tuple(threading.Lock() for _ in range(256))


def _publish_lock(target: Path) -> threading.Lock:
    normalized_target = os.path.normcase(os.fspath(target))
    return _PUBLISH_LOCKS[hash(normalized_target) % len(_PUBLISH_LOCKS)]


@dataclass(frozen=True)
class RawArtifactReference:
    content_sha256: str
    relative_path: str
    compression: str
    media_type: str
    uncompressed_bytes: int
    compressed_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class RawArtifactStore:
    """Write immutable raw bytes using temp+fsync+atomic rename."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser().resolve(strict=False)
        self.root.mkdir(parents=True, exist_ok=True)

    def put_json(self, value: Any, *, media_type: str = "application/json") -> RawArtifactReference:
        payload = canonical_json(value, exclude_volatile=False).encode("utf-8")
        return self.put_bytes(payload, media_type=media_type, extension="json")

    def put_bytes(
        self,
        payload: bytes,
        *,
        media_type: str = "application/octet-stream",
        extension: str = "bin",
    ) -> RawArtifactReference:
        content = bytes(payload)
        content_hash = sha256_hex(content)
        safe_extension = extension.strip().lower().lstrip(".")
        if not safe_extension or not safe_extension.replace("_", "").isalnum():
            raise ValueError("extension must contain only letters, digits, or underscores")
        target_dir = self.root / content_hash[:2]
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / f"{content_hash}.{safe_extension}.gz"

        with _publish_lock(target):
            # Recheck while holding the process-wide target lock. Another
            # RawArtifactStore instance may have published the same immutable
            # artifact after this caller calculated its path.
            if not target.exists():
                descriptor, temp_name = tempfile.mkstemp(
                    prefix=f".{content_hash}.",
                    suffix=".tmp",
                    dir=target_dir,
                )
                temp = Path(temp_name)
                try:
                    with os.fdopen(descriptor, "wb") as raw_handle:
                        with gzip.GzipFile(
                            filename="",
                            mode="wb",
                            fileobj=raw_handle,
                            mtime=0,
                        ) as zipped:
                            zipped.write(content)
                        raw_handle.flush()
                        os.fsync(raw_handle.fileno())
                    os.replace(temp, target)
                    self._fsync_directory(target_dir)
                except BaseException:
                    try:
                        temp.unlink(missing_ok=True)
                    except OSError:
                        pass
                    raise

        restored = self.read_path(target)
        if sha256_hex(restored) != content_hash:
            raise IOError(f"raw artifact hash verification failed: {target.name}")
        return RawArtifactReference(
            content_sha256=content_hash,
            relative_path=target.relative_to(self.root).as_posix(),
            compression="gzip",
            media_type=str(media_type),
            uncompressed_bytes=len(content),
            compressed_bytes=target.stat().st_size,
        )

    def read(self, reference: RawArtifactReference | dict[str, Any]) -> bytes:
        values = reference.to_dict() if isinstance(reference, RawArtifactReference) else reference
        relative_path = str(values["relative_path"])
        target = (self.root / relative_path).resolve(strict=True)
        if self.root != target and self.root not in target.parents:
            raise ValueError("raw artifact reference escapes configured root")
        payload = self.read_path(target)
        if sha256_hex(payload) != str(values["content_sha256"]):
            raise IOError("raw artifact content hash does not match its reference")
        return payload

    @staticmethod
    def read_path(path: Path) -> bytes:
        with gzip.open(path, "rb") as handle:
            return handle.read()

    @staticmethod
    def _fsync_directory(directory: Path) -> None:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        descriptor: int | None = None
        try:
            descriptor = os.open(directory, flags)
            os.fsync(descriptor)
        except OSError:
            # Windows cannot fsync directories. The file itself was already
            # fsynced before the atomic replacement.
            return
        finally:
            if descriptor is not None:
                os.close(descriptor)


__all__ = ["RawArtifactReference", "RawArtifactStore"]
