#!/usr/bin/env python3
"""Build and compare secret-safe manifests for the Docker application source tree.

The backend source profile covers root-level Python modules plus the ``api``,
``bot``, ``data_provider``, ``src``, ``strategies`` and report ``templates``
trees copied by the Dockerfile.  Compiled Web ``static/`` files use a separate
asset profile so source and build evidence cannot be mistaken for each other.
Neither profile walks arbitrary runtime data or credential mounts under
``/app``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence


FORMAT_VERSION = 1
PROFILE_NAME = "docker-app-source-v2"
STATIC_PROFILE_NAME = "docker-static-assets-v1"
SOURCE_DIRECTORIES = ("api", "bot", "data_provider", "src", "strategies", "templates")
ROOT_FILES = ("requirements.txt",)
IGNORED_DIRECTORY_NAMES = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    "build",
    "coverage",
    "dist",
    "node_modules",
}
SENSITIVE_EXACT_NAMES = {
    ".env",
    "credentials.json",
    "runtime.env",
    "secrets.json",
}
SENSITIVE_SUFFIXES = (".cer", ".crt", ".der", ".key", ".p12", ".pem", ".pfx")
TEXT_SUFFIXES = (
    ".cfg",
    ".css",
    ".html",
    ".ini",
    ".j2",
    ".js",
    ".json",
    ".map",
    ".md",
    ".py",
    ".svg",
    ".toml",
    ".ts",
    ".txt",
    ".webmanifest",
    ".xml",
    ".yaml",
    ".yml",
)
TEXT_EXACT_NAMES = ("license",)


class ManifestError(RuntimeError):
    """Raised when a source manifest cannot be produced safely."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _content_fingerprint(path: Path) -> tuple[str, int, str]:
    content = path.read_bytes()
    if path.suffix.lower() in TEXT_SUFFIXES or path.name.lower() in TEXT_EXACT_NAMES:
        content = content.replace(b"\r\n", b"\n")
        hash_mode = "lf_normalized_text"
    else:
        hash_mode = "raw_bytes"
    return hashlib.sha256(content).hexdigest(), len(content), hash_mode


def _is_sensitive_name(path: Path) -> bool:
    name = path.name.lower()
    return (
        name in SENSITIVE_EXACT_NAMES
        or name.startswith(".env.")
        or name.startswith("secrets.")
        or name.endswith(SENSITIVE_SUFFIXES)
    )


def _is_ignored_relative_path(relative_path: Path) -> bool:
    return any(part.lower() in IGNORED_DIRECTORY_NAMES for part in relative_path.parts)


def _candidate_paths(root: Path) -> Iterable[Path]:
    for root_module in sorted(root.glob("*.py")):
        yield root_module

    for filename in ROOT_FILES:
        path = root / filename
        if path.exists():
            yield path

    for dirname in SOURCE_DIRECTORIES:
        directory = root / dirname
        if not directory.is_dir():
            continue
        for path in sorted(directory.rglob("*")):
            if path.is_file() or path.is_symlink():
                yield path


def _entry_for_path(root: Path, path: Path, *, ignore_source_directories: bool = True) -> dict[str, Any] | None:
    relative = path.relative_to(root)
    if ignore_source_directories and _is_ignored_relative_path(relative):
        return None
    if _is_sensitive_name(relative):
        # Fail closed before reading either file contents or a symlink target.
        # The error intentionally omits the sensitive filename.
        raise ManifestError("sensitive file detected inside selected manifest profile")

    relative_name = relative.as_posix()
    if path.is_symlink():
        # Hash the link target for drift detection without printing a possibly
        # host-specific or sensitive absolute target.
        target_digest = hashlib.sha256(os.readlink(path).encode("utf-8", errors="surrogateescape")).hexdigest()
        return {
            "path": relative_name,
            "sha256": target_digest,
            "size_bytes": 0,
            "type": "symlink",
            "hash_mode": "link_target",
        }

    content_hash, content_size, hash_mode = _content_fingerprint(path)
    return {
        "path": relative_name,
        "sha256": content_hash,
        "size_bytes": content_size,
        "type": "file",
        "hash_mode": hash_mode,
    }


def build_source_manifest(root: Path) -> dict[str, Any]:
    """Return a deterministic manifest for the Docker application source profile."""

    root = root.resolve()
    if not root.is_dir():
        raise ManifestError("source root does not exist or is not a directory")

    entries_by_path: dict[str, dict[str, Any]] = {}
    for path in _candidate_paths(root):
        entry = _entry_for_path(root, path)
        if entry is not None:
            entries_by_path[entry["path"]] = entry

    entries = [entries_by_path[name] for name in sorted(entries_by_path)]
    canonical = json.dumps(entries, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {
        "format_version": FORMAT_VERSION,
        "profile": PROFILE_NAME,
        "generated_at_utc": _utc_now(),
        "file_count": len(entries),
        "profile_sha256": hashlib.sha256(canonical).hexdigest(),
        "entries": entries,
    }


def build_static_asset_manifest(static_root: Path) -> dict[str, Any]:
    """Return a manifest for one already-built ``static/`` artifact tree."""

    static_root = static_root.resolve()
    if not static_root.is_dir():
        raise ManifestError("static asset root does not exist or is not a directory")

    entries_by_path: dict[str, dict[str, Any]] = {}
    for path in sorted(static_root.rglob("*")):
        if not (path.is_file() or path.is_symlink()):
            continue
        entry = _entry_for_path(static_root, path, ignore_source_directories=False)
        if entry is not None:
            entries_by_path[entry["path"]] = entry

    entries = [entries_by_path[name] for name in sorted(entries_by_path)]
    canonical = json.dumps(entries, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {
        "format_version": FORMAT_VERSION,
        "profile": STATIC_PROFILE_NAME,
        "generated_at_utc": _utc_now(),
        "file_count": len(entries),
        "profile_sha256": hashlib.sha256(canonical).hexdigest(),
        "entries": entries,
    }


def _compare_manifests(expected_manifest: dict[str, Any], deployed_manifest: dict[str, Any]) -> dict[str, Any]:
    expected_entries = {entry["path"]: entry for entry in expected_manifest["entries"]}
    deployed_entries = {entry["path"]: entry for entry in deployed_manifest["entries"]}

    expected_paths = set(expected_entries)
    deployed_paths = set(deployed_entries)
    missing = sorted(expected_paths - deployed_paths)
    unexpected = sorted(deployed_paths - expected_paths)
    mismatches = []
    for path in sorted(expected_paths & deployed_paths):
        expected_entry = expected_entries[path]
        deployed_entry = deployed_entries[path]
        if (expected_entry["sha256"], expected_entry["type"]) != (
            deployed_entry["sha256"],
            deployed_entry["type"],
        ):
            mismatches.append(
                {
                    "path": path,
                    "expected_sha256": expected_entry["sha256"],
                    "deployed_sha256": deployed_entry["sha256"],
                    "expected_type": expected_entry["type"],
                    "deployed_type": deployed_entry["type"],
                }
            )

    return {
        "matches": not missing and not unexpected and not mismatches,
        "expected": {
            "file_count": expected_manifest["file_count"],
            "profile_sha256": expected_manifest["profile_sha256"],
        },
        "deployed": {
            "file_count": deployed_manifest["file_count"],
            "profile_sha256": deployed_manifest["profile_sha256"],
        },
        "missing_in_deployed": missing,
        "unexpected_in_deployed": unexpected,
        "content_mismatches": mismatches,
    }


def compare_source_roots(repo_root: Path, deployed_root: Path) -> dict[str, Any]:
    """Compare repository and exported runtime roots without exposing file contents."""

    repo_manifest = build_source_manifest(repo_root)
    deployed_manifest = build_source_manifest(deployed_root)
    comparison = _compare_manifests(repo_manifest, deployed_manifest)
    return {
        "format_version": FORMAT_VERSION,
        "profile": PROFILE_NAME,
        "compared_at_utc": _utc_now(),
        "matches": comparison["matches"],
        "repo": comparison["expected"],
        "deployed": comparison["deployed"],
        "missing_in_deployed": comparison["missing_in_deployed"],
        "unexpected_in_deployed": comparison["unexpected_in_deployed"],
        "content_mismatches": comparison["content_mismatches"],
    }


def compare_static_asset_roots(expected_static_root: Path, deployed_static_root: Path) -> dict[str, Any]:
    """Compare two already-built static trees independently from source code."""

    expected_manifest = build_static_asset_manifest(expected_static_root)
    deployed_manifest = build_static_asset_manifest(deployed_static_root)
    comparison = _compare_manifests(expected_manifest, deployed_manifest)
    return {
        "format_version": FORMAT_VERSION,
        "profile": STATIC_PROFILE_NAME,
        "compared_at_utc": _utc_now(),
        **comparison,
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
    subparsers = parser.add_subparsers(dest="command", required=True)

    manifest = subparsers.add_parser("manifest", help="create a source manifest")
    manifest.add_argument("--root", type=Path, required=True, help="repository or exported /app root")
    manifest.add_argument("--output", default="-", help="JSON path or '-' for stdout")

    compare = subparsers.add_parser("compare", help="compare repository and exported /app roots")
    compare.add_argument("--repo-root", type=Path, required=True)
    compare.add_argument("--deployed-root", type=Path, required=True)
    compare.add_argument("--output", default="-", help="JSON path or '-' for stdout")

    assets = subparsers.add_parser("asset-manifest", help="create a manifest for an already-built static tree")
    assets.add_argument("--static-root", type=Path, required=True)
    assets.add_argument("--output", default="-", help="JSON path or '-' for stdout")

    compare_assets = subparsers.add_parser("compare-assets", help="compare two already-built static trees")
    compare_assets.add_argument("--expected-static-root", type=Path, required=True)
    compare_assets.add_argument("--deployed-static-root", type=Path, required=True)
    compare_assets.add_argument("--output", default="-", help="JSON path or '-' for stdout")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "manifest":
            payload = build_source_manifest(args.root)
            exit_code = 0
        elif args.command == "compare":
            payload = compare_source_roots(args.repo_root, args.deployed_root)
            exit_code = 0 if payload["matches"] else 1
        elif args.command == "asset-manifest":
            payload = build_static_asset_manifest(args.static_root)
            exit_code = 0
        else:
            payload = compare_static_asset_roots(args.expected_static_root, args.deployed_static_root)
            exit_code = 0 if payload["matches"] else 1
        _write_json(payload, args.output)
        return exit_code
    except (ManifestError, OSError) as exc:
        print(f"source manifest failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
