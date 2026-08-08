"""Fail-closed validation helpers for immutable research evidence.

The evidence layer stores provider prose as data and only keeps display-safe
URL references.  These helpers deliberately avoid DNS or HTTP access: URLs
are normalized for attribution, never dereferenced.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import ipaddress
import re
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit, urlunsplit

from .canonical import canonical_hash


MAX_EVIDENCE_ITEMS = 16
MAX_EVIDENCE_CLAIMS = 32
MAX_EVIDENCE_TITLE_CHARS = 300
MAX_EVIDENCE_EXCERPT_CHARS = 800
MAX_EVIDENCE_PROMPT_CHARS = 12_000

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_INVALID_POINTER_ESCAPE_RE = re.compile(r"~(?![01])")
_UNSAFE_HOST_SUFFIXES = (".localhost", ".local", ".internal", ".home", ".lan")
_DNS_LABEL_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_CANONICAL_PATH_RE = re.compile(r"^/_/sha256-[0-9a-f]{64}$")


def require_sha256(value: Any, *, field: str) -> str:
    """Return a lowercase SHA-256 digest or reject the reference."""

    if not isinstance(value, str):
        raise TypeError(f"{field} must be a lowercase SHA-256 digest")
    normalized = value.strip()
    if not _SHA256_RE.fullmatch(normalized):
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return normalized


def require_identifier(value: Any, *, field: str) -> str:
    normalized = str(value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(normalized):
        raise ValueError(f"{field} must be a non-empty safe identifier")
    return normalized


def require_aware_utc(value: Any, *, field: str) -> datetime:
    """Parse an RFC3339 instant while rejecting unknown/naive time."""

    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError(f"{field} is required")
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{field} must be an RFC3339 datetime") from exc
    else:
        raise TypeError(f"{field} must be a datetime or RFC3339 string")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a UTC offset")
    return parsed.astimezone(timezone.utc)


def bounded_text(
    value: Any,
    *,
    field: str,
    max_chars: int,
    required: bool = False,
) -> str:
    """Normalize control characters and enforce a hard character cap."""

    text = "" if value is None else str(value)
    text = _CONTROL_RE.sub(" ", text).replace("\r\n", "\n").replace("\r", "\n").strip()
    if required and not text:
        raise ValueError(f"{field} is required")
    if len(text) > max_chars:
        text = text[:max_chars].rstrip()
    return text


def _public_host(hostname: str) -> str:
    raw = hostname.strip().rstrip(".").casefold()
    if not raw or any(character.isspace() for character in raw):
        raise ValueError("canonical_url requires a valid public hostname")
    try:
        address = ipaddress.ip_address(raw)
    except ValueError:
        try:
            host = raw.encode("idna").decode("ascii")
        except UnicodeError as exc:
            raise ValueError("canonical_url hostname is invalid") from exc
        if host == "localhost" or host.endswith(_UNSAFE_HOST_SUFFIXES):
            raise ValueError("canonical_url private hostnames are forbidden")
        # A dotless hostname is not a stable public attribution boundary.
        if "." not in host:
            raise ValueError("canonical_url requires a public hostname")
        labels = host.split(".")
        if any(not _DNS_LABEL_RE.fullmatch(label) for label in labels):
            raise ValueError("canonical_url hostname is invalid")
        # Browsers may interpret dotted decimal/octal/hex spellings as IPs even
        # when Python's strict ipaddress parser does not.  A public attribution
        # hostname therefore requires a non-numeric final DNS label.
        if labels[-1].isdigit() or all(
            re.fullmatch(r"(?:0x[0-9a-f]+|[0-9]+)", label) for label in labels
        ):
            raise ValueError("canonical_url numeric hostnames are forbidden")
        return host
    if not address.is_global:
        raise ValueError("canonical_url private or reserved IP addresses are forbidden")
    return address.compressed


def safe_canonical_url(value: Any, *, field: str = "canonical_url") -> str:
    """Return a display-safe HTTP(S) URL without credentials or path secrets."""

    raw = bounded_text(value, field=field, max_chars=4096, required=True)
    try:
        parsed = urlsplit(raw)
    except ValueError as exc:
        raise ValueError(f"{field} is invalid") from exc
    scheme = parsed.scheme.casefold()
    if scheme not in {"http", "https"}:
        raise ValueError(f"{field} only supports http(s) URLs")
    if parsed.username is not None or parsed.password is not None or "@" in parsed.netloc:
        raise ValueError(f"{field} must not contain userinfo")
    if parsed.hostname is None:
        raise ValueError(f"{field} requires a hostname")
    host = _public_host(parsed.hostname)
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError(f"{field} contains an invalid port") from exc
    default_port = (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    rendered_host = f"[{host}]" if ":" in host else host
    netloc = rendered_host if port is None or default_port else f"{rendered_host}:{port}"
    path = parsed.path or "/"
    if path != "/" and not _CANONICAL_PATH_RE.fullmatch(path):
        path_hash = hashlib.sha256(
            b"dsa-research-evidence-url-path\0" + path.encode("utf-8")
        ).hexdigest()
        path = f"/_/sha256-{path_hash}"
    return urlunsplit((scheme, netloc, path, "", ""))


def validate_json_pointer(value: Any, *, field: str = "json_pointer") -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be an RFC6901 string")
    if len(value) > 1024:
        raise ValueError(f"{field} exceeds 1024 characters")
    if value and not value.startswith("/"):
        raise ValueError(f"{field} must be an RFC6901 JSON pointer")
    if _INVALID_POINTER_ESCAPE_RE.search(value):
        raise ValueError(f"{field} contains an invalid RFC6901 escape")
    return value


def resolve_json_pointer(document: Any, pointer: Any) -> Any:
    """Resolve a strict RFC6901 pointer without object attribute traversal."""

    normalized = validate_json_pointer(pointer)
    current = document
    if normalized == "":
        return current
    for raw_token in normalized[1:].split("/"):
        token = raw_token.replace("~1", "/").replace("~0", "~")
        if isinstance(current, Mapping):
            if token not in current:
                raise ValueError(f"json_pointer does not exist: {normalized}")
            current = current[token]
            continue
        if isinstance(current, Sequence) and not isinstance(current, (str, bytes, bytearray)):
            # RFC6901 array indexes are canonical unsigned decimal integers.
            if not token.isdigit() or (len(token) > 1 and token.startswith("0")):
                raise ValueError(f"json_pointer has an invalid array index: {normalized}")
            index = int(token)
            if index >= len(current):
                raise ValueError(f"json_pointer does not exist: {normalized}")
            current = current[index]
            continue
        raise ValueError(f"json_pointer crosses a scalar value: {normalized}")
    return current


def json_value_hash(document: Any, pointer: Any) -> str:
    return canonical_hash(
        resolve_json_pointer(document, pointer),
        exclude_volatile=False,
    )


__all__ = [
    "MAX_EVIDENCE_CLAIMS",
    "MAX_EVIDENCE_EXCERPT_CHARS",
    "MAX_EVIDENCE_ITEMS",
    "MAX_EVIDENCE_PROMPT_CHARS",
    "MAX_EVIDENCE_TITLE_CHARS",
    "bounded_text",
    "json_value_hash",
    "require_aware_utc",
    "require_identifier",
    "require_sha256",
    "resolve_json_pointer",
    "safe_canonical_url",
    "validate_json_pointer",
]
