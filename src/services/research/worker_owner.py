"""Cross-process ownership lock for the single Tushare research worker."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, BinaryIO, Optional

from sqlalchemy.engine import make_url


class TushareWorkerOwnerError(RuntimeError):
    """Raised when a process cannot become the only Tushare research owner."""


def tushare_owner_lock_path(database_url: str) -> Path:
    """Resolve the zero-content lock file adjacent to a file-backed SQLite DB."""

    url = make_url(str(database_url))
    if url.get_backend_name() != "sqlite" or not url.database:
        raise TushareWorkerOwnerError(
            "Tushare research owner requires a file-backed SQLite database"
        )
    database = str(url.database).strip()
    if not database or database == ":memory:" or database.startswith("file::memory:"):
        raise TushareWorkerOwnerError(
            "Tushare research owner requires a file-backed SQLite database"
        )
    database_path = Path(database).expanduser()
    if not database_path.is_absolute():
        database_path = database_path.resolve(strict=False)
    return database_path.with_name(f"{database_path.name}.tushare-owner.lock")


class TushareWorkerOwnerLock:
    """Non-blocking advisory lock whose empty file can survive process crashes."""

    def __init__(self, lock_path: Path) -> None:
        self.lock_path = Path(lock_path).expanduser().resolve(strict=False)
        self._handle: Optional[BinaryIO] = None

    @classmethod
    def from_database_url(cls, database_url: str) -> "TushareWorkerOwnerLock":
        return cls(tushare_owner_lock_path(database_url))

    @property
    def acquired(self) -> bool:
        return self._handle is not None

    def acquire(self) -> "TushareWorkerOwnerLock":
        """Acquire immediately or fail without waiting for the existing owner."""

        if self._handle is not None:
            return self
        try:
            self.lock_path.parent.mkdir(parents=True, exist_ok=True)
            handle: Optional[BinaryIO] = self.lock_path.open("a+b")
        except OSError as exc:
            raise TushareWorkerOwnerError(
                "Tushare owner lock file cannot be opened; verify database directory access"
            ) from exc
        try:
            handle.seek(0)
            _lock_file(handle)
        except OSError as exc:
            handle.close()
            raise TushareWorkerOwnerError(
                "another Durable Worker already owns Tushare research access"
            ) from exc
        self._handle = handle
        return self

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return
        self._handle = None
        try:
            handle.seek(0)
            _unlock_file(handle)
        finally:
            handle.close()

    def __enter__(self) -> "TushareWorkerOwnerLock":
        return self.acquire()

    def __exit__(self, *_args: Any) -> None:
        self.release()


def acquire_tushare_worker_owner(
    database_url: str,
    *,
    enabled: bool,
) -> Optional[TushareWorkerOwnerLock]:
    """Acquire the production owner only when research access is enabled."""

    if not bool(enabled):
        return None
    return TushareWorkerOwnerLock.from_database_url(database_url).acquire()


def _lock_file(handle: BinaryIO) -> None:
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        return
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_file(handle: BinaryIO) -> None:
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


__all__ = [
    "TushareWorkerOwnerError",
    "TushareWorkerOwnerLock",
    "acquire_tushare_worker_owner",
    "tushare_owner_lock_path",
]
