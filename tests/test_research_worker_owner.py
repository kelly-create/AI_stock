"""Single-owner contracts for Durable Worker Tushare access."""

from __future__ import annotations

import builtins
import os
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

import src.services.durable_worker as durable_worker_module
from src.services.research.worker_owner import (
    TushareWorkerOwnerError,
    TushareWorkerOwnerLock,
    acquire_tushare_worker_owner,
    tushare_owner_lock_path,
)
from src.services.durable_worker import DurableWorker, build_worker_from_runtime


def _sqlite_url(path: Path) -> str:
    return f"sqlite:///{path.resolve()}"


class _RuntimeStore:
    heartbeat_seconds = 0.1
    lease_seconds = 5.0

    def __init__(self, *, fail_start: bool = False) -> None:
        self.fail_start = fail_start

    def recover_expired_leases(self) -> int:
        if self.fail_start:
            raise RuntimeError("runtime start fixture")
        return 0

    def claim_next(self, _worker_id):
        return None

    def heartbeat_component(self, *_args, **_kwargs) -> None:
        return None


class _RuntimeOutboxDispatcher:
    def __init__(self, *_args, **_kwargs) -> None:
        pass

    def dispatch_once(self):
        return SimpleNamespace(status="idle")

    def dispatch_available(self, *, max_messages: int):
        assert max_messages == 16
        return []


def _patch_runtime_builder(
    monkeypatch,
    *,
    research_enabled: bool,
    store: _RuntimeStore,
) -> SimpleNamespace:
    import src.config as config_module
    import src.services.durable_jobs as durable_jobs_module
    import src.services.notification_outbox_dispatcher as outbox_module
    import src.storage as storage_module

    config = SimpleNamespace(
        durable_jobs_enabled=True,
        tushare_research_enabled=research_enabled,
        database_migration_mode="auto",
        max_workers=1,
    )
    monkeypatch.setattr(config_module, "get_config", lambda: config)
    monkeypatch.setattr(durable_worker_module, "preflight_worker", lambda *_args: None)
    monkeypatch.setattr(storage_module, "DatabaseManager", lambda _url: object())
    monkeypatch.setattr(
        durable_worker_module,
        "build_default_durable_job_registry",
        lambda: object(),
    )
    monkeypatch.setattr(
        durable_worker_module,
        "DurableJobStore",
        lambda _registry, _db_manager: store,
    )
    monkeypatch.setattr(
        durable_jobs_module,
        "NotificationOutboxStore",
        lambda _db_manager: object(),
    )
    monkeypatch.setattr(
        outbox_module,
        "NotificationOutboxDispatcher",
        _RuntimeOutboxDispatcher,
    )
    return config


def test_zero_byte_residual_is_recoverable_and_second_owner_fails_fast(tmp_path) -> None:
    database_url = _sqlite_url(tmp_path / "research.db")
    lock_path = tushare_owner_lock_path(database_url)
    lock_path.write_bytes(b"")

    first = TushareWorkerOwnerLock.from_database_url(database_url).acquire()
    second = TushareWorkerOwnerLock.from_database_url(database_url)
    try:
        assert first.acquired is True
        assert lock_path.stat().st_size == 0
        with pytest.raises(TushareWorkerOwnerError, match="already owns"):
            second.acquire()
    finally:
        first.release()

    second.acquire()
    assert second.acquired is True
    second.release()
    assert lock_path.stat().st_size == 0


def test_lock_is_cross_process_and_released_for_next_worker(tmp_path) -> None:
    database_url = _sqlite_url(tmp_path / "cross-process.db")
    owner = TushareWorkerOwnerLock.from_database_url(database_url).acquire()
    script = (
        "import sys; "
        "from src.services.research.worker_owner import "
        "TushareWorkerOwnerError,TushareWorkerOwnerLock; "
        "lock=TushareWorkerOwnerLock.from_database_url(sys.argv[1]); "
        "\ntry: lock.acquire()\n"
        "except TushareWorkerOwnerError: raise SystemExit(7)\n"
        "else: lock.release(); raise SystemExit(0)"
    )
    try:
        blocked = subprocess.run(
            [sys.executable, "-c", script, database_url],
            cwd=Path.cwd(),
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert blocked.returncode == 7, blocked.stderr
    finally:
        owner.release()

    recovered = subprocess.run(
        [sys.executable, "-c", script, database_url],
        cwd=Path.cwd(),
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert recovered.returncode == 0, recovered.stderr


def test_disabled_research_does_not_create_or_acquire_lock(tmp_path) -> None:
    database_url = _sqlite_url(tmp_path / "disabled.db")
    lock_path = tushare_owner_lock_path(database_url)

    assert acquire_tushare_worker_owner(database_url, enabled=False) is None
    assert not lock_path.exists()


@pytest.mark.parametrize(
    "database_url",
    ["sqlite:///:memory:", "postgresql://example.invalid/dsa"],
)
def test_enabled_owner_rejects_non_file_sqlite_database(database_url: str) -> None:
    with pytest.raises(TushareWorkerOwnerError, match="file-backed SQLite"):
        acquire_tushare_worker_owner(database_url, enabled=True)


def test_context_manager_releases_after_failure(tmp_path) -> None:
    database_url = _sqlite_url(tmp_path / "failure.db")

    with pytest.raises(RuntimeError, match="fixture"):
        with TushareWorkerOwnerLock.from_database_url(database_url):
            raise RuntimeError("fixture")

    replacement = TushareWorkerOwnerLock.from_database_url(database_url).acquire()
    replacement.release()


def test_durable_worker_shutdown_releases_owner_for_replacement(tmp_path) -> None:
    class EmptyStore:
        heartbeat_seconds = 1.0
        lease_seconds = 5.0

        def recover_expired_leases(self) -> int:
            return 0

        def claim_next(self, _worker_id):
            return None

        def heartbeat_component(self, *_args, **_kwargs) -> None:
            return None

    database_url = _sqlite_url(tmp_path / "worker-lifecycle.db")
    owner = TushareWorkerOwnerLock.from_database_url(database_url).acquire()
    worker = DurableWorker(
        EmptyStore(),  # type: ignore[arg-type]
        worker_id="fixture-owner",
        poll_interval_seconds=0.01,
        heartbeat_interval_seconds=0.1,
        owner_lock=owner,
    )

    worker.run_until_idle()

    assert owner.acquired is False
    replacement = TushareWorkerOwnerLock.from_database_url(database_url).acquire()
    replacement.release()


def test_durable_worker_start_failure_still_releases_owner(tmp_path) -> None:
    class FailingStore:
        heartbeat_seconds = 1.0
        lease_seconds = 5.0

        def recover_expired_leases(self) -> int:
            raise RuntimeError("startup fixture")

        def heartbeat_component(self, *_args, **_kwargs) -> None:
            return None

    database_url = _sqlite_url(tmp_path / "worker-start-failure.db")
    owner = TushareWorkerOwnerLock.from_database_url(database_url).acquire()
    worker = DurableWorker(
        FailingStore(),  # type: ignore[arg-type]
        heartbeat_interval_seconds=0.1,
        owner_lock=owner,
    )

    with pytest.raises(RuntimeError, match="startup fixture"):
        worker.run_until_idle()

    assert owner.acquired is False
    replacement = TushareWorkerOwnerLock.from_database_url(database_url).acquire()
    replacement.release()


def test_runtime_builder_flag_off_never_imports_or_creates_owner_lock(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _sqlite_url(tmp_path / "runtime-disabled.db")
    lock_path = tushare_owner_lock_path(database_url)
    config = _patch_runtime_builder(
        monkeypatch,
        research_enabled=False,
        store=_RuntimeStore(),
    )
    imported_owner_modules: list[str] = []
    original_import = builtins.__import__

    def tracking_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "src.services.research.worker_owner":
            imported_owner_modules.append(name)
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", tracking_import)

    worker = build_worker_from_runtime(database_url=database_url, worker_id="flag-off")
    worker.shutdown(wait=True)

    assert imported_owner_modules == []
    assert not lock_path.exists()
    assert config.database_migration_mode == "explicit"


def test_runtime_builder_constructor_failure_releases_owner_lock(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _sqlite_url(tmp_path / "runtime-constructor-failure.db")
    _patch_runtime_builder(
        monkeypatch,
        research_enabled=True,
        store=_RuntimeStore(),
    )

    with pytest.raises(ValueError, match="max_workers"):
        build_worker_from_runtime(database_url=database_url, max_workers=0)

    replacement = TushareWorkerOwnerLock.from_database_url(database_url).acquire()
    replacement.release()


def test_runtime_builder_start_failure_releases_owner_lock(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _sqlite_url(tmp_path / "runtime-start-failure.db")
    _patch_runtime_builder(
        monkeypatch,
        research_enabled=True,
        store=_RuntimeStore(fail_start=True),
    )
    worker = build_worker_from_runtime(database_url=database_url, worker_id="start-failure")

    with pytest.raises(RuntimeError, match="runtime start fixture"):
        worker.run_until_idle()

    replacement = TushareWorkerOwnerLock.from_database_url(database_url).acquire()
    replacement.release()


def test_runtime_builder_normal_shutdown_releases_owner_lock(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = _sqlite_url(tmp_path / "runtime-shutdown.db")
    _patch_runtime_builder(
        monkeypatch,
        research_enabled=True,
        store=_RuntimeStore(),
    )
    worker = build_worker_from_runtime(database_url=database_url, worker_id="normal-shutdown")

    worker.run_until_idle()

    replacement = TushareWorkerOwnerLock.from_database_url(database_url).acquire()
    replacement.release()


def test_production_builder_is_single_process_owner_and_recovers_after_kill(tmp_path) -> None:
    from src.migrations import apply_migrations

    database_path = tmp_path / "production-owner.db"
    database_url = _sqlite_url(database_path)
    migration_state = apply_migrations(database_url)
    assert migration_state.is_current is True

    lock_path = tushare_owner_lock_path(database_url)
    lock_path.write_bytes(b"")
    environment = os.environ.copy()
    environment.update(
        {
            "DATABASE_PATH": str(database_path),
            "DATABASE_MIGRATION_MODE": "explicit",
            "DURABLE_JOBS_ENABLED": "true",
            "PERSONAL_RESEARCH_ENABLED": "true",
            "TUSHARE_RESEARCH_ENABLED": "true",
            "TUSHARE_TOKEN": "test-worker-owner-token",
        }
    )

    def worker_command(worker_id: str, *, once: bool = False) -> list[str]:
        command = [
            sys.executable,
            "-m",
            "src.services.durable_worker",
            "--database-url",
            database_url,
            "--worker-id",
            worker_id,
            "--workers",
            "1",
            "--poll-interval",
            "0.02",
            "--log-level",
            "ERROR",
        ]
        if once:
            command.append("--once")
        return command

    owner_process = subprocess.Popen(
        worker_command("production-owner-a"),
        cwd=Path.cwd(),
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            if owner_process.poll() is not None:
                stdout, stderr = owner_process.communicate()
                pytest.fail(
                    "production owner exited before acquiring its lock: "
                    f"stdout={stdout!r} stderr={stderr!r}"
                )
            probe = TushareWorkerOwnerLock.from_database_url(database_url)
            try:
                probe.acquire()
            except TushareWorkerOwnerError:
                break
            else:
                probe.release()
                time.sleep(0.05)
        else:
            pytest.fail("production owner did not acquire its lock within 15 seconds")

        contender = subprocess.run(
            worker_command("production-owner-b", once=True),
            cwd=Path.cwd(),
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
        assert contender.returncode == 1, contender.stderr
        assert "another Durable Worker already owns" in (contender.stdout + contender.stderr)
    finally:
        if owner_process.poll() is None:
            owner_process.kill()
        owner_process.communicate(timeout=10)

    assert lock_path.is_file()
    assert lock_path.stat().st_size == 0

    recovered = subprocess.run(
        worker_command("production-owner-after-kill", once=True),
        cwd=Path.cwd(),
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert recovered.returncode == 0, recovered.stderr
    assert lock_path.stat().st_size == 0
