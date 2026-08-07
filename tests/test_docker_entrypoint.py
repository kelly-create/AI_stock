import os
import re
import subprocess
import sys
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_docker_entrypoint_has_valid_shell_syntax() -> None:
    for script_name in ("entrypoint.sh", "healthcheck.sh"):
        subprocess.run(
            ["sh", "-n", str(REPO_ROOT / "docker" / script_name)],
            check=True,
        )


def test_dockerfile_uses_entrypoint_to_drop_privileges() -> None:
    dockerfile = (REPO_ROOT / "docker" / "Dockerfile").read_text(encoding="utf-8")

    assert "gosu" in dockerfile
    assert 'ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]' in dockerfile
    assert "USER dsa" not in dockerfile


def test_dockerfile_healthcheck_uses_configured_api_port_and_readiness() -> None:
    dockerfile = (REPO_ROOT / "docker" / "Dockerfile").read_text(encoding="utf-8")
    healthcheck = (REPO_ROOT / "docker" / "healthcheck.sh").read_text(encoding="utf-8")

    assert "COPY docker/healthcheck.sh /usr/local/bin/dsa-healthcheck" in dockerfile
    assert "chmod +x /usr/local/bin/docker-entrypoint.sh /usr/local/bin/dsa-healthcheck" in dockerfile
    assert 'CMD ["/usr/local/bin/dsa-healthcheck"]' in dockerfile
    assert "${API_PORT:-8000}" in healthcheck
    assert "/api/v1/health/ready" in healthcheck
    assert "sys.exit(0)" not in dockerfile
    assert "|| true" not in healthcheck
    assert 'CMD ["python", "main.py", "--schedule"]' in dockerfile


def test_compose_enables_mode_aware_healthcheck_for_api_and_scheduler() -> None:
    compose_path = REPO_ROOT / "docker" / "docker-compose.yml"
    compose = yaml.safe_load(compose_path.read_text(encoding="utf-8"))

    for service_name in ("analyzer", "server"):
        healthcheck = compose["services"][service_name].get("healthcheck", {})
        assert healthcheck.get("disable") is not True


def test_compose_assigns_scheduler_ownership_only_to_analyzer() -> None:
    compose_path = REPO_ROOT / "docker" / "docker-compose.yml"
    compose = yaml.safe_load(compose_path.read_text(encoding="utf-8"))

    analyzer_env = compose["services"]["analyzer"]["environment"]
    server_env = compose["services"]["server"]["environment"]
    assert "DSA_RUNTIME_SCHEDULER_SUPPRESS_START" not in analyzer_env
    assert server_env["DSA_RUNTIME_SCHEDULER_SUPPRESS_START"] == "true"
    assert server_env["DATABASE_MIGRATION_MODE"] == "explicit"
    assert server_env["WEBUI_HOST"] == "0.0.0.0"


def test_compose_runs_single_migrator_before_api_and_scheduler() -> None:
    compose_path = REPO_ROOT / "docker" / "docker-compose.yml"
    compose = yaml.safe_load(compose_path.read_text(encoding="utf-8"))

    migrator = compose["services"]["migrator"]
    assert migrator["command"] == ["python", "-m", "src.migrations", "--apply"]
    assert migrator["restart"] == "no"
    assert migrator["healthcheck"]["disable"] is True
    assert compose["x-common"]["environment"]["DATABASE_MIGRATION_MODE"] == "explicit"
    for service_name in ("analyzer", "server"):
        assert compose["services"][service_name]["depends_on"] == {
            "migrator": {"condition": "service_completed_successfully"}
        }


def test_dockerfile_bundles_builtin_screening_engine() -> None:
    dockerfile = (REPO_ROOT / "docker" / "Dockerfile").read_text(encoding="utf-8")
    requirements = (REPO_ROOT / "requirements.txt").read_text(encoding="utf-8")

    assert "screening.git" not in requirements.lower()
    assert "pip install -r requirements.txt" in dockerfile
    assert "--mount=type=cache,target=/root/.cache/pip" in dockerfile
    assert "import src.services.screening.pipeline" in dockerfile


def test_docker_entrypoint_repairs_ownership_and_user_permissions() -> None:
    entrypoint = (REPO_ROOT / "docker" / "entrypoint.sh").read_text(encoding="utf-8")

    assert "directory_needs_repair" in entrypoint
    assert "has_unwritable_mount_path" in entrypoint
    assert "can_write_dir_as_app_user" in entrypoint
    assert "DATABASE_FILE" in entrypoint
    assert "/home/dsa/.longbridge" in entrypoint
    assert 'HOME="/home/dsa"' in entrypoint
    assert re.search(r"export\s+HOME\s+exec\s+gosu", entrypoint, re.DOTALL)
    assert re.search(r"\bchown\s+-R\b", entrypoint)
    assert re.search(r"\bchmod\s+-R\s+u\+rwX\b", entrypoint)
    assert re.search(r"gosu\s+\"\$APP_USER:\$APP_GROUP\"\s+test\s+-w", entrypoint)


def test_docker_compose_injects_env_without_single_file_env_mount() -> None:
    compose_text = (REPO_ROOT / "docker" / "docker-compose.yml").read_text(encoding="utf-8")
    compose = yaml.safe_load(compose_text)
    common = compose["x-common"]

    assert "../.env" in common["env_file"]
    assert "../.env:/app/.env" not in common["volumes"]
    assert not any(str(volume).startswith("../.env:") for volume in common["volumes"])
    assert "../longbridge_tokens:/home/dsa/.longbridge" in common["volumes"]


def test_docker_compose_default_memory_recommendation_is_not_512m() -> None:
    compose_text = (REPO_ROOT / "docker" / "docker-compose.yml").read_text(encoding="utf-8")
    compose = yaml.safe_load(compose_text)
    resources = compose["x-common"]["deploy"]["resources"]

    assert resources["limits"]["memory"] == "1G"
    assert resources["reservations"]["memory"] == "512M"
    assert "512M" in compose_text
    assert "MAX_WORKERS=1" in compose_text


def test_docker_memory_guides_describe_resource_profiles() -> None:
    doc_paths = (
        "docs/DEPLOY.md",
        "docs/DEPLOY_EN.md",
        "docs/full-guide.md",
        "docs/full-guide_EN.md",
        "docs/docker/zeabur-deployment.md",
    )

    for doc_path in doc_paths:
        doc = (REPO_ROOT / doc_path).read_text(encoding="utf-8")

        assert "512M" in doc
        assert "1G" in doc
        assert "2G+" in doc
        assert "MAX_WORKERS=1" in doc


def test_docker_guides_do_not_recommend_single_file_env_bind_mount() -> None:
    forbidden_mount_patterns = [
        r"\$\(pwd\)/\.env:/app/\.env",
        r"\.\./\.env:/app/\.env",
    ]

    for doc_path in ("docs/full-guide.md", "docs/full-guide_EN.md"):
        doc = (REPO_ROOT / doc_path).read_text(encoding="utf-8")

        assert "--env-file .env" in doc
        assert "env_file:" in doc
        for pattern in forbidden_mount_patterns:
            assert re.search(pattern, doc) is None


def test_documented_compose_exec_commands_run_as_dsa() -> None:
    safe_exec_prefix = "docker-compose -f ./docker/docker-compose.yml exec -u dsa"
    unsafe_exec_prefix = "docker-compose -f ./docker/docker-compose.yml exec"

    for doc_path in ("docs/DEPLOY.md", "docs/DEPLOY_EN.md"):
        doc = (REPO_ROOT / doc_path).read_text(encoding="utf-8")

        assert f"{safe_exec_prefix} stock-analyzer bash" in doc
        assert f"{safe_exec_prefix} stock-analyzer python main.py --no-notify" in doc
        assert f"{unsafe_exec_prefix} stock-analyzer bash" not in doc
        assert (
            f"{unsafe_exec_prefix} stock-analyzer python main.py --no-notify"
            not in doc
        )


def _write_fake_command(fakebin: Path, name: str, body: str) -> None:
    command = fakebin / name
    command.write_text(f"#!/bin/sh\n{body}", encoding="utf-8")
    command.chmod(0o755)


def _write_synthetic_proc_command(
    root: Path,
    *command: str,
    state: str = "S",
) -> Path:
    proc_root = root / "proc"
    process_dir = proc_root / str(os.getpid())
    process_dir.mkdir(parents=True)
    process_dir.joinpath("cmdline").write_bytes(
        b"\0".join(part.encode("utf-8") for part in command) + b"\0"
    )
    process_dir.joinpath("status").write_text(
        f"Name:\tdsa-test\nState:\t{state} (test)\n",
        encoding="utf-8",
    )
    return proc_root


def _run_healthcheck(
    tmp_path: Path,
    proc_root: Path,
    *,
    curl_exit: int = 0,
    api_port: str = "18080",
    webui_enabled: str = "false",
) -> tuple[subprocess.CompletedProcess[str], Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    fakebin = tmp_path / "health-bin"
    fakebin.mkdir()
    curl_log = tmp_path / "curl.log"
    _write_fake_command(
        fakebin,
        "curl",
        'printf "%s\\n" "$*" > "$HEALTHCHECK_CURL_LOG"\n'
        'exit "$FAKE_CURL_EXIT"\n',
    )
    env = os.environ.copy()
    env["PATH"] = f"{fakebin}:{env['PATH']}"
    env["HEALTHCHECK_CURL_LOG"] = str(curl_log)
    env["FAKE_CURL_EXIT"] = str(curl_exit)
    env["API_PORT"] = api_port
    env["WEBUI_ENABLED"] = webui_enabled
    result = subprocess.run(
        [
            "sh",
            str(REPO_ROOT / "docker" / "healthcheck.sh"),
            str(proc_root),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    return result, curl_log


def test_healthcheck_default_scheduler_mode_checks_live_dsa_process(tmp_path: Path) -> None:
    proc_root = _write_synthetic_proc_command(
        tmp_path,
        "python",
        "main.py",
        "--schedule",
    )

    result, curl_log = _run_healthcheck(tmp_path, proc_root, curl_exit=22)

    assert result.returncode == 0
    assert not curl_log.exists()


def test_healthcheck_scans_real_proc_and_signals_live_scheduler(tmp_path: Path) -> None:
    if not Path("/proc").is_dir():
        return
    scheduler = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import time; time.sleep(30)",
            "main.py",
            "--schedule",
        ]
    )
    try:
        result, curl_log = _run_healthcheck(tmp_path, Path("/proc"), curl_exit=22)
    finally:
        scheduler.terminate()
        scheduler.wait(timeout=5)

    assert result.returncode == 0
    assert not curl_log.exists()


def test_healthcheck_api_mode_requires_configured_port_readiness(tmp_path: Path) -> None:
    proc_root = _write_synthetic_proc_command(
        tmp_path,
        "python",
        "/app/main.py",
        "--serve-only",
    )

    result, curl_log = _run_healthcheck(
        tmp_path,
        proc_root,
        curl_exit=0,
        api_port="18765",
    )

    assert result.returncode == 0
    curl_args = curl_log.read_text(encoding="utf-8")
    assert "--max-time 8" in curl_args
    assert "http://127.0.0.1:18765/api/v1/health/ready" in curl_args


def test_healthcheck_api_mode_propagates_readiness_failure(tmp_path: Path) -> None:
    proc_root = _write_synthetic_proc_command(
        tmp_path,
        "python",
        "main.py",
        "--serve",
        "--schedule",
    )

    result, curl_log = _run_healthcheck(tmp_path, proc_root, curl_exit=22)

    assert result.returncode == 22
    assert curl_log.exists()


def test_healthcheck_webui_env_makes_scheduler_command_require_readiness(tmp_path: Path) -> None:
    proc_root = _write_synthetic_proc_command(
        tmp_path,
        "python",
        "main.py",
        "--schedule",
    )

    result, curl_log = _run_healthcheck(
        tmp_path,
        proc_root,
        curl_exit=22,
        webui_enabled="TrUe",
    )

    assert result.returncode == 22
    assert curl_log.exists()


def test_healthcheck_rejects_unrelated_or_zombie_processes(tmp_path: Path) -> None:
    unrelated_root = _write_synthetic_proc_command(tmp_path / "unrelated", "sleep", "999")
    unrelated, _ = _run_healthcheck(tmp_path / "unrelated-run", unrelated_root)

    zombie_root = _write_synthetic_proc_command(
        tmp_path / "zombie",
        "python",
        "main.py",
        "--schedule",
        state="Z",
    )
    zombie, _ = _run_healthcheck(tmp_path / "zombie-run", zombie_root)

    assert unrelated.returncode == 1
    assert "no live DSA process" in unrelated.stderr
    assert zombie.returncode == 1
    assert "no live DSA process" in zombie.stderr


def _prepare_fake_entrypoint_tools(tmp_path: Path, find_body: str) -> tuple[Path, Path]:
    fakebin = tmp_path / "bin"
    log_dir = tmp_path / "logs"
    fakebin.mkdir()
    log_dir.mkdir()

    _write_fake_command(
        fakebin,
        "id",
        'if [ "${1:-}" = "-u" ]; then printf "0\\n"; else printf "0\\n"; fi\n',
    )
    _write_fake_command(fakebin, "mkdir", "exit 0\n")
    _write_fake_command(fakebin, "find", find_body)
    _write_fake_command(
        fakebin,
        "chown",
        'printf "%s\\n" "$*" >> "$FAKE_LOG_DIR/chown.log"\n'
        'exit "${CHOWN_EXIT:-0}"\n',
    )
    _write_fake_command(
        fakebin,
        "chmod",
        'printf "%s\\n" "$*" >> "$FAKE_LOG_DIR/chmod.log"\n'
        'exit "${CHMOD_EXIT:-0}"\n',
    )
    _write_fake_command(
        fakebin,
        "gosu",
        'shift\n'
        'case "$1" in\n'
        '    sh|test) exit "${GOSU_WRITE_EXIT:-0}" ;;\n'
        'esac\n'
        'exec "$@"\n',
    )

    return fakebin, log_dir


def _run_entrypoint_with_fake_tools(
    fakebin: Path,
    log_dir: Path,
    *,
    gosu_write_exit: int,
    chown_exit: int,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PATH"] = f"{fakebin}:{env['PATH']}"
    env["FAKE_LOG_DIR"] = str(log_dir)
    env["GOSU_WRITE_EXIT"] = str(gosu_write_exit)
    env["CHOWN_EXIT"] = str(chown_exit)

    return subprocess.run(
        ["sh", str(REPO_ROOT / "docker" / "entrypoint.sh"), "true"],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )


def test_docker_entrypoint_repairs_nested_mount_ownership(tmp_path: Path) -> None:
    fakebin, log_dir = _prepare_fake_entrypoint_tools(
        tmp_path,
        'for arg in "$@"; do\n'
        '    if [ "$arg" = "-maxdepth" ]; then exit 0; fi\n'
        "done\n"
        'printf "%s/nested-root-owned\\n" "$1"\n',
    )

    _run_entrypoint_with_fake_tools(
        fakebin,
        log_dir,
        gosu_write_exit=0,
        chown_exit=0,
    )

    chown_log = (log_dir / "chown.log").read_text(encoding="utf-8")
    chmod_log = (log_dir / "chmod.log").read_text(encoding="utf-8")
    assert "/app/data" in chown_log
    assert "/app/logs" in chown_log
    assert "/app/reports" in chown_log
    assert "/app/data" in chmod_log


def test_docker_entrypoint_skips_owner_chmod_when_chown_fails(tmp_path: Path) -> None:
    fakebin, log_dir = _prepare_fake_entrypoint_tools(
        tmp_path,
        'printf "%s/root-owned\\n" "$1"\n',
    )

    result = _run_entrypoint_with_fake_tools(
        fakebin,
        log_dir,
        gosu_write_exit=1,
        chown_exit=1,
    )

    assert (log_dir / "chown.log").exists()
    assert not (log_dir / "chmod.log").exists()
    assert "skipping owner-only chmod" in result.stderr
    assert "still not writable by dsa" in result.stderr
