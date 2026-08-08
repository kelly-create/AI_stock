"""Stable code fingerprints for source and frozen application runtimes."""

from __future__ import annotations

import hashlib
import inspect
import marshal
from types import CodeType, ModuleType
from typing import Any


def _identity(value: Any) -> str:
    module_name = str(getattr(value, "__module__", "") or "")
    qualname = str(
        getattr(value, "__qualname__", getattr(value, "__name__", "")) or ""
    )
    if isinstance(value, ModuleType):
        module_name = value.__name__
        qualname = "<module>"
    return f"{module_name}:{qualname}:{type(value).__name__}"


def _loader_code(value: Any) -> CodeType | None:
    module = value if isinstance(value, ModuleType) else inspect.getmodule(value)
    if module is None:
        return None
    loader = getattr(module, "__loader__", None)
    if loader is None:
        spec = getattr(module, "__spec__", None)
        loader = getattr(spec, "loader", None)
    get_code = getattr(loader, "get_code", None)
    if not callable(get_code):
        return None
    try:
        code = get_code(module.__name__)
    except (ImportError, OSError, TypeError):
        return None
    return code if isinstance(code, CodeType) else None


def _callable_code(value: Any) -> CodeType | None:
    candidate = getattr(value, "__func__", value)
    code = getattr(candidate, "__code__", None)
    if isinstance(code, CodeType):
        return code
    return _loader_code(value)


def fingerprint_code(*values: Any, version: str) -> str:
    """Hash source when available and bytecode/loader code in frozen builds.

    The explicit version is always included and is the final deterministic
    fallback when neither source nor executable code can be inspected. This
    keeps PyInstaller/FrozenImporter builds operational without ever emitting
    an empty or process-random fingerprint.
    """

    version_text = str(version or "").strip()
    if not version_text:
        raise ValueError("code fingerprint version is required")
    hasher = hashlib.sha256()
    chunks = [b"research-code-fingerprint-v1", version_text.encode("utf-8")]
    for value in values:
        identity = _identity(value).encode("utf-8")
        try:
            payload = b"source\0" + inspect.getsource(value).encode("utf-8")
        except (OSError, TypeError):
            code = _callable_code(value)
            if code is not None:
                payload = b"marshal\0" + marshal.dumps(code)
            else:
                payload = b"versioned-identity\0" + identity
        chunks.extend((identity, payload))
    for chunk in chunks:
        hasher.update(len(chunk).to_bytes(8, "big"))
        hasher.update(chunk)
    return hasher.hexdigest()


__all__ = ["fingerprint_code"]
