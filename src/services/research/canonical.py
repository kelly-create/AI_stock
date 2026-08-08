"""Deterministic JSON canonicalization for immutable research artifacts."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
import hashlib
import json
import math
from typing import Any, Mapping


VOLATILE_RESEARCH_KEYS = frozenset(
    {
        "job",
        "job_id",
        "origin_job_id",
        "durable_job_id",
        "trace",
        "trace_id",
        "created",
        "created_at",
        "updated_at",
        "latency",
        "latency_ms",
        # Explicit legacy aliases. These are exact metadata field names, not
        # tokens: business fields such as ``job_growth`` remain hash-bearing.
        "originjobid",
        "durablejobid",
        "traceid",
        "createdat",
        "updatedat",
        "latencyms",
        "provider_latency_ms",
        "providerlatencyms",
    }
)


class CanonicalJSONError(ValueError):
    """Raised when a value cannot belong to a stable JSON snapshot."""


def _datetime_text(value: datetime) -> str:
    if value.tzinfo is not None and value.utcoffset() is not None:
        value = value.astimezone(timezone.utc)
        return value.isoformat().replace("+00:00", "Z")
    return value.isoformat()


def _is_volatile_key(value: str) -> bool:
    # Only explicitly declared runtime metadata is volatile. Substring/token
    # matching silently erased legitimate research dimensions (for example
    # ``job_growth`` or ``created_value``) from immutable identities.
    return value.strip().casefold() in VOLATILE_RESEARCH_KEYS


def _normalize(value: Any, *, exclude_volatile: bool) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        value = asdict(value)
    if isinstance(value, Enum):
        value = value.value
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CanonicalJSONError("NaN and infinite values are forbidden")
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise CanonicalJSONError("NaN and infinite values are forbidden")
        if value == value.to_integral_value():
            return int(value)
        converted = float(value)
        if not math.isfinite(converted):
            raise CanonicalJSONError("decimal value exceeds finite JSON range")
        return converted
    if isinstance(value, datetime):
        return _datetime_text(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise CanonicalJSONError("JSON object keys must be strings")
            if exclude_volatile and _is_volatile_key(key):
                continue
            normalized[key] = _normalize(item, exclude_volatile=exclude_volatile)
        return normalized
    if isinstance(value, (list, tuple)):
        return [_normalize(item, exclude_volatile=exclude_volatile) for item in value]
    raise CanonicalJSONError(
        f"unsupported canonical JSON value: {type(value).__name__}"
    )


def canonicalize(value: Any, *, exclude_volatile: bool = True) -> Any:
    """Return a JSON-compatible tree with unstable execution metadata removed."""

    return _normalize(value, exclude_volatile=exclude_volatile)


def canonical_json(value: Any, *, exclude_volatile: bool = True) -> str:
    """Serialize a value as stable UTF-8 JSON text."""

    normalized = canonicalize(value, exclude_volatile=exclude_volatile)
    try:
        return json.dumps(
            normalized,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise CanonicalJSONError(str(exc)) from exc


def sha256_hex(value: bytes | str) -> str:
    payload = value.encode("utf-8") if isinstance(value, str) else bytes(value)
    return hashlib.sha256(payload).hexdigest()


def canonical_hash(value: Any, *, exclude_volatile: bool = True) -> str:
    return sha256_hex(canonical_json(value, exclude_volatile=exclude_volatile))


__all__ = [
    "CanonicalJSONError",
    "VOLATILE_RESEARCH_KEYS",
    "canonical_hash",
    "canonical_json",
    "canonicalize",
    "sha256_hex",
]
