#!/usr/bin/env python3
"""Capture a secret-safe, read-only production baseline as JSON."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

try:
    from scripts.source_manifest import build_source_manifest
except ModuleNotFoundError:  # pragma: no cover - direct ``python scripts/...`` execution
    from source_manifest import build_source_manifest


FORMAT_VERSION = 1
DEFAULT_CORE_TABLES = (
    "schema_migrations",
    "stock_daily",
    "analysis_history",
    "intelligence_items",
    "llm_usage",
    "portfolio_accounts",
    "portfolio_trades",
    "portfolio_cash_ledger",
    "portfolio_positions",
    "portfolio_position_lots",
    "portfolio_daily_snapshots",
    "decision_signals",
    "decision_signal_outcomes",
    "skill_opinion_samples",
    "skill_opinion_outcomes",
)


class BaselineError(RuntimeError):
    """Raised when a requested baseline input cannot be captured."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    """Hash a file without parsing or returning its contents."""

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _run_checked(command: list[str], *, cwd: Path | None = None) -> str:
    try:
        completed = subprocess.run(
            command,
            cwd=str(cwd) if cwd else None,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        # Do not include captured stdout/stderr: commands such as image inspect
        # must never accidentally copy unrelated runtime metadata into reports.
        raise BaselineError(f"command failed: {command[0]} {command[1] if len(command) > 1 else ''}".strip()) from exc
    return completed.stdout.strip()


def collect_git_metadata(repo_root: Path) -> dict[str, Any]:
    commit = _run_checked(["git", "rev-parse", "HEAD"], cwd=repo_root)
    branch = _run_checked(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=repo_root)
    dirty = bool(_run_checked(["git", "status", "--porcelain", "--untracked-files=normal"], cwd=repo_root))
    return {"commit": commit, "branch": branch, "dirty": dirty}


def _docker_field(kind: str, target: str, template: str) -> str:
    return _run_checked(["docker", kind, "inspect", "--format", template, target])


def collect_image_metadata(*, image_ref: str | None = None, container: str | None = None) -> dict[str, Any] | None:
    """Inspect only non-sensitive image identity fields, never full Docker metadata."""

    if not image_ref and not container:
        return None
    if image_ref and container:
        raise BaselineError("choose either image_ref or container, not both")

    if container:
        resolved_ref = _docker_field("container", container, "{{.Config.Image}}")
        image_id = _docker_field("container", container, "{{.Image}}")
    else:
        resolved_ref = str(image_ref)
        image_id = _docker_field("image", resolved_ref, "{{.Id}}")

    repo_digests_raw = _docker_field("image", image_id, "{{json .RepoDigests}}")
    revision = _docker_field(
        "image",
        image_id,
        '{{if .Config.Labels}}{{index .Config.Labels "org.opencontainers.image.revision"}}{{end}}',
    )
    try:
        repo_digests = json.loads(repo_digests_raw) if repo_digests_raw and repo_digests_raw != "null" else []
    except json.JSONDecodeError as exc:
        raise BaselineError("Docker returned an invalid RepoDigests value") from exc

    if revision == "<no value>":
        revision = ""
    return {
        "reference": resolved_ref,
        "image_id": image_id,
        "repo_digests": sorted(str(value) for value in repo_digests),
        "revision_label": revision or None,
    }


def _artifact_label(path: Path, repo_root: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        # Do not disclose host directory layouts for runtime.env or exported
        # production artifacts located outside the repository.
        return path.name


def collect_fingerprints(paths: Sequence[Path], repo_root: Path) -> list[dict[str, str]]:
    fingerprints = []
    labels: set[str] = set()
    for path in paths:
        if not path.is_file():
            raise BaselineError(f"fingerprint input is not a file: {path.name}")
        label = _artifact_label(path, repo_root)
        if label in labels:
            raise BaselineError(f"duplicate fingerprint label: {label}")
        labels.add(label)
        fingerprints.append({"name": label, "sha256": sha256_file(path)})
    return sorted(fingerprints, key=lambda item: item["name"])


def _schema_object_rows(connection: sqlite3.Connection) -> list[tuple[str, str, str, str]]:
    rows = connection.execute(
        """
        SELECT type, name, tbl_name, COALESCE(sql, '')
        FROM sqlite_master
        WHERE (name NOT LIKE 'sqlite_%' OR type = 'index')
          AND type IN ('table', 'index', 'view', 'trigger')
        ORDER BY type, name
        """
    ).fetchall()
    return [(str(kind), str(name), str(table_name), str(sql)) for kind, name, table_name, sql in rows]


def _schema_summary(rows: Sequence[tuple[str, str, str, str]]) -> dict[str, Any]:
    canonical = json.dumps(rows, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    objects = []
    for kind, name, table_name, sql in rows:
        objects.append(
            {
                "type": kind,
                "name": name,
                "table": table_name,
                "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
            }
        )
    return {
        "object_count": len(objects),
        "sha256": hashlib.sha256(canonical).hexdigest(),
        "objects": objects,
    }


def _quoted_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def inspect_sqlite_database(database_path: Path, core_tables: Sequence[str]) -> dict[str, Any]:
    """Collect schema and integrity facts through a read-only SQLite connection."""

    database_path = database_path.resolve()
    if not database_path.is_file():
        raise BaselineError("database path does not exist or is not a file")

    uri = f"{database_path.as_uri()}?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=5.0)
    except sqlite3.Error as exc:
        raise BaselineError("unable to open SQLite database in read-only mode") from exc

    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        rows = _schema_object_rows(connection)
        table_names = {name for kind, name, _table_name, _sql in rows if kind == "table"}

        counts: dict[str, dict[str, Any]] = {}
        for table_name in dict.fromkeys(core_tables):
            if table_name not in table_names:
                counts[table_name] = {"status": "absent", "count": None}
                continue
            count = connection.execute(f"SELECT COUNT(*) FROM {_quoted_identifier(table_name)}").fetchone()[0]
            counts[table_name] = {"status": "present", "count": int(count)}

        quick_rows = connection.execute("PRAGMA quick_check").fetchall()
        quick_ok = len(quick_rows) == 1 and str(quick_rows[0][0]).lower() == "ok"
        foreign_key_rows = connection.execute("PRAGMA foreign_key_check").fetchall()
        schema_version = int(connection.execute("PRAGMA schema_version").fetchone()[0])
        user_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        connection.rollback()
    except sqlite3.Error as exc:
        raise BaselineError("SQLite baseline query failed") from exc
    finally:
        connection.close()

    schema = _schema_summary(rows)
    indexes = [item for item in schema["objects"] if item["type"] == "index"]
    return {
        "name": database_path.name,
        "open_mode": "read_only",
        "schema_version": schema_version,
        "user_version": user_version,
        "schema": schema,
        "indexes": {"count": len(indexes), "objects": indexes},
        "core_table_counts": counts,
        "quick_check": {"status": "ok" if quick_ok else "failed", "issue_count": 0 if quick_ok else len(quick_rows)},
        "foreign_key_check": {
            "status": "ok" if not foreign_key_rows else "failed",
            "violation_count": len(foreign_key_rows),
        },
    }


def evaluate_core_table_acceptance(
    database_snapshot: dict[str, Any],
    required_core_tables: Sequence[str],
    *,
    strict: bool,
) -> dict[str, Any]:
    """Evaluate required production tables without performing any database writes."""

    counts = database_snapshot.get("core_table_counts", {})
    required = list(dict.fromkeys(required_core_tables))
    missing = [table for table in required if counts.get(table, {}).get("status") != "present"]
    quick_check = database_snapshot.get("quick_check", {})
    foreign_key_check = database_snapshot.get("foreign_key_check", {})
    failed_integrity_checks = []
    if quick_check.get("status") != "ok" or quick_check.get("issue_count") != 0:
        failed_integrity_checks.append("quick_check")
    if foreign_key_check.get("status") != "ok" or foreign_key_check.get("violation_count") != 0:
        failed_integrity_checks.append("foreign_key_check")

    blocking = bool(failed_integrity_checks or (strict and missing))
    if blocking:
        status = "failed"
    elif missing:
        status = "warning"
    else:
        status = "passed"
    return {
        "mode": "strict" if strict else "diagnostic",
        "status": status,
        "blocking": blocking,
        "required_core_tables": required,
        "missing_required_core_tables": missing,
        "failed_integrity_checks": failed_integrity_checks,
    }


def evaluate_identity_acceptance(
    database_acceptance: dict[str, Any],
    *,
    image_metadata: dict[str, Any] | None,
    compose_artifacts: Sequence[dict[str, Any]],
    config_artifacts: Sequence[dict[str, Any]],
    strict: bool,
) -> dict[str, Any]:
    """Require reproducible deployment identity for a production baseline."""

    missing = []
    if image_metadata is None:
        missing.append("image")
    if not compose_artifacts:
        missing.append("compose")
    if not config_artifacts:
        missing.append("config")

    acceptance = dict(database_acceptance)
    acceptance["identity_mode"] = "strict" if strict else "diagnostic"
    acceptance["missing_identity_artifacts"] = missing
    if missing and strict:
        acceptance["status"] = "failed"
        acceptance["blocking"] = True
    elif missing and not acceptance.get("blocking") and acceptance.get("status") == "passed":
        acceptance["status"] = "warning"
    return acceptance


def capture_baseline(
    *,
    repo_root: Path,
    database_path: Path,
    compose_files: Sequence[Path],
    config_files: Sequence[Path],
    core_tables: Sequence[str] = DEFAULT_CORE_TABLES,
    image_ref: str | None = None,
    container: str | None = None,
    strict_required_tables: bool = True,
    strict_identity: bool = True,
) -> dict[str, Any]:
    source_manifest = build_source_manifest(repo_root)
    database_snapshot = inspect_sqlite_database(database_path, core_tables)
    image_metadata = collect_image_metadata(image_ref=image_ref, container=container)
    compose_artifacts = collect_fingerprints(compose_files, repo_root)
    config_artifacts = collect_fingerprints(config_files, repo_root)
    database_acceptance = evaluate_core_table_acceptance(
        database_snapshot,
        core_tables,
        strict=strict_required_tables,
    )
    return {
        "format_version": FORMAT_VERSION,
        "captured_at_utc": _utc_now(),
        "git": collect_git_metadata(repo_root),
        "image": image_metadata,
        "artifacts": {
            "compose": compose_artifacts,
            # The byte stream is hashed directly. No config key or value is
            # parsed, retained, logged or emitted.
            "config": config_artifacts,
        },
        "source": {
            "profile": source_manifest["profile"],
            "file_count": source_manifest["file_count"],
            "profile_sha256": source_manifest["profile_sha256"],
        },
        "database": database_snapshot,
        "acceptance": evaluate_identity_acceptance(
            database_acceptance,
            image_metadata=image_metadata,
            compose_artifacts=compose_artifacts,
            config_artifacts=config_artifacts,
            strict=strict_identity,
        ),
    }


def _write_json(payload: dict[str, Any], output: str) -> None:
    rendered = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if output == "-":
        sys.stdout.write(rendered)
        return
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered, encoding="utf-8")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--database", type=Path, required=True, help="SQLite database to inspect read-only")
    parser.add_argument("--compose", action="append", type=Path, default=[], help="Compose file to fingerprint")
    parser.add_argument(
        "--config-file",
        action="append",
        type=Path,
        default=[],
        help="configuration file to fingerprint without parsing or printing values",
    )
    parser.add_argument("--core-table", action="append", default=[], help="override the default core-table allowlist")
    parser.add_argument(
        "--allow-missing-core-tables",
        action="store_true",
        help="diagnostic mode: allow missing tables only; integrity failures remain blocking",
    )
    parser.add_argument(
        "--diagnostic",
        action="store_true",
        help=(
            "allow omitted image, Compose, or config identity for offline inspection; "
            "database integrity failures remain blocking"
        ),
    )
    image_group = parser.add_mutually_exclusive_group()
    image_group.add_argument("--image-ref", help="local Docker image reference to inspect")
    image_group.add_argument("--container", help="running Docker container whose image identity is captured")
    parser.add_argument("--output", default="-", help="JSON path or '-' for stdout")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        payload = capture_baseline(
            repo_root=args.repo_root,
            database_path=args.database,
            compose_files=args.compose,
            config_files=args.config_file,
            core_tables=args.core_table or DEFAULT_CORE_TABLES,
            image_ref=args.image_ref,
            container=args.container,
            strict_required_tables=not args.allow_missing_core_tables,
            strict_identity=not args.diagnostic,
        )
        _write_json(payload, args.output)
        return 1 if payload["acceptance"]["blocking"] else 0
    except (BaselineError, OSError) as exc:
        print(f"baseline capture failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
