"""Pure, bounded contract validation for Research Factor artifacts.

The factor engine, durable writer, stored-row reader, and public API all expose
the same derived payload.  This module gives those boundaries one reusable
validator without importing storage or API models.  Writer inputs may use
ordinary ``list``/``tuple`` containers and unordered Dataset references; read
and API boundaries can additionally require the already-canonical order.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import math
import re
from types import MappingProxyType
from typing import Any, Mapping, Optional, Sequence

from src.utils.sanitize import sanitize_decision_signal_payload

from .canonical import canonical_json, canonicalize
from .evidence_security import require_sha256
from .schemas import parse_datetime


MAX_FACTOR_ARTIFACT_JSON_BYTES = 256 * 1024
MAX_FACTOR_TREE_DEPTH = 12
MAX_FACTOR_TREE_NODES = 10_000
MAX_FACTOR_MAPPING_ITEMS = 256
MAX_FACTOR_SEQUENCE_ITEMS = 1_024
MAX_FACTOR_STRING_CHARS = 4_096
MAX_FACTOR_KEY_CHARS = 128
MAX_FACTOR_UNKNOWNS = 512
MAX_FACTOR_DATASET_HASHES = 512

FACTOR_COMPONENT_SCORE_FIELDS = MappingProxyType(
    {
        "value": "value_score",
        "quality": "quality_score",
        "trend_timing": "trend_score",
        "catalyst": "catalyst_score",
        "risk": "risk_penalty",
    }
)

_FACTOR_COMPONENTS = frozenset(FACTOR_COMPONENT_SCORE_FIELDS)
_UNKNOWN_KEYS = frozenset({"component", "metric", "reason"})
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_TEMPORAL_KEY_GROUPS = {
    "announcement_date": frozenset(
        {
            "actual_ann_date",
            "ann_date",
            "announced_at",
            "announcement_date",
            "f_ann_date",
            "imp_ann_date",
        }
    ),
    "as_of": frozenset({"as_of", "asof"}),
    "available_at": frozenset({"available_at", "availableat"}),
    "created_at": frozenset({"created_at"}),
    "data_as_of": frozenset({"data_as_of", "dataasof"}),
    "fetched_at": frozenset({"fetched_at"}),
    "knowledge_as_of": frozenset({"knowledge_as_of", "knowledgeasof"}),
    "observed_at": frozenset({"observed_at"}),
    "provider_timestamp": frozenset({"provider_timestamp"}),
    "published_at": frozenset(
        {
            "pub_date",
            "publication_date",
            "publication_time",
            "publish_date",
            "published_at",
        }
    ),
    "timestamp": frozenset({"timestamp"}),
    "trade_date": frozenset({"trade_date"}),
    "updated_at": frozenset({"updated_at"}),
}
_TEMPORAL_KEY_ALIASES = {
    alias: canonical
    for canonical, aliases in _TEMPORAL_KEY_GROUPS.items()
    for alias in aliases
}
_TEMPORAL_COMPACT_KEY_ALIASES = {
    alias.replace("_", ""): canonical
    for alias, canonical in _TEMPORAL_KEY_ALIASES.items()
}


@dataclass(frozen=True)
class ValidatedFactorContract:
    """Canonical, immutable projection shared by writer/read/API boundaries."""

    factor_payload: Mapping[str, Any]
    unknowns: tuple[Mapping[str, str], ...]
    input_dataset_hashes: tuple[str, ...]
    as_of: datetime
    available_at: datetime
    value_score: Optional[float]
    quality_score: Optional[float]
    trend_score: Optional[float]
    catalyst_score: Optional[float]
    risk_penalty: Optional[float]


def _utc_datetime(value: Any, *, field: str) -> datetime:
    try:
        parsed = parse_datetime(value, field=field)
    except (TypeError, ValueError) as exc:
        raise type(exc)(str(exc)) from exc
    return parsed.astimezone(timezone.utc)


def _snake_key(value: str) -> str:
    """Normalize snake/camel/provider field spellings before classification."""

    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value.strip())
    return re.sub(r"[^a-z0-9]+", "_", text.casefold()).strip("_")


def _temporal_key(value: str) -> Optional[str]:
    normalized = _snake_key(value)
    return _TEMPORAL_KEY_ALIASES.get(
        normalized,
        _TEMPORAL_COMPACT_KEY_ALIASES.get(normalized.replace("_", "")),
    )


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType(
            {str(key): _deep_freeze(item) for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(_deep_freeze(item) for item in value)
    return value


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def _whitespace_projection(value: Any) -> Any:
    """Mirror sanitizer whitespace normalization without hiding redactions."""

    if isinstance(value, Mapping):
        return {
            str(key): _whitespace_projection(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_whitespace_projection(item) for item in value]
    if isinstance(value, str):
        return " ".join(value.strip().split())
    return value


def _validate_public_tree(value: Any, *, field: str) -> None:
    sanitized = sanitize_decision_signal_payload(value)
    if sanitized != _whitespace_projection(value):
        raise ValueError(f"{field} contains secret-like material")


def _bounded_json_tree(
    value: Any,
    *,
    field: str,
    depth: int,
    counter: list[int],
) -> Any:
    if depth > MAX_FACTOR_TREE_DEPTH:
        raise ValueError(f"{field} exceeds maximum nesting depth")
    counter[0] += 1
    if counter[0] > MAX_FACTOR_TREE_NODES:
        raise ValueError(f"{field} exceeds maximum JSON node count")

    if isinstance(value, Mapping):
        if len(value) > MAX_FACTOR_MAPPING_ITEMS:
            raise ValueError(
                f"{field} exceeds {MAX_FACTOR_MAPPING_ITEMS} object members"
            )
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{field} object keys must be strings")
            if not key or len(key) > MAX_FACTOR_KEY_CHARS:
                raise ValueError(
                    f"{field} object keys must contain 1 to "
                    f"{MAX_FACTOR_KEY_CHARS} characters"
                )
            if _CONTROL_RE.search(key):
                raise ValueError(f"{field} object key contains control characters")
            normalized[key] = _bounded_json_tree(
                item,
                field=f"{field}.{key}",
                depth=depth + 1,
                counter=counter,
            )
        return normalized

    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        if len(value) > MAX_FACTOR_SEQUENCE_ITEMS:
            raise ValueError(
                f"{field} exceeds {MAX_FACTOR_SEQUENCE_ITEMS} array items"
            )
        return [
            _bounded_json_tree(
                item,
                field=f"{field}[{index}]",
                depth=depth + 1,
                counter=counter,
            )
            for index, item in enumerate(value)
        ]

    if isinstance(value, str):
        if len(value) > MAX_FACTOR_STRING_CHARS:
            raise ValueError(
                f"{field} exceeds {MAX_FACTOR_STRING_CHARS} characters"
            )
        if _CONTROL_RE.search(value):
            raise ValueError(f"{field} contains control characters")
        return value

    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{field} must not contain NaN or infinity")
        return value
    if isinstance(value, (date, datetime)):
        return canonicalize(value, exclude_volatile=False)

    # Preserve canonical Decimal/Enum support without allowing arbitrary
    # objects or ``default=str`` coercion at a public boundary.
    try:
        scalar = canonicalize(value, exclude_volatile=False)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"{field} contains an unsupported JSON value") from exc
    if isinstance(scalar, (Mapping, list)):
        raise TypeError(f"{field} contains an unsupported JSON value")
    if isinstance(scalar, str):
        if len(scalar) > MAX_FACTOR_STRING_CHARS:
            raise ValueError(
                f"{field} exceeds {MAX_FACTOR_STRING_CHARS} characters"
            )
        if _CONTROL_RE.search(scalar):
            raise ValueError(f"{field} contains control characters")
    if isinstance(scalar, float) and not math.isfinite(scalar):
        raise ValueError(f"{field} must not contain NaN or infinity")
    return scalar


def _validate_time_bounds(
    value: Any,
    *,
    as_of: datetime,
    field: str,
    root: bool = False,
) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            path = f"{field}.{key}"
            temporal_key = _temporal_key(key)
            if temporal_key is not None and item is not None:
                instant = _utc_datetime(item, field=path)
                if instant > as_of:
                    raise ValueError(f"{path} cannot be after as_of")
                if root and temporal_key == "as_of" and instant != as_of:
                    raise ValueError(f"{path} must match the outer as_of")
            _validate_time_bounds(item, as_of=as_of, field=path)
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _validate_time_bounds(
                item,
                as_of=as_of,
                field=f"{field}[{index}]",
            )


def _score(value: Any, *, field: str) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field} must be a finite number or null")
    normalized = float(value)
    if not math.isfinite(normalized) or not 0.0 <= normalized <= 100.0:
        raise ValueError(f"{field} must be finite and between zero and 100")
    return normalized


def validate_factor_payload(
    value: Any,
    *,
    as_of: Any,
    field: str = "factor_payload",
) -> Mapping[str, Any]:
    """Validate and freeze one bounded, public-safe Factor payload mapping."""

    converter = getattr(value, "to_dict", None)
    if not isinstance(value, Mapping) and callable(converter):
        value = converter()
    if not isinstance(value, Mapping):
        raise TypeError(f"{field} must be a mapping or expose to_dict()")
    boundary = _utc_datetime(as_of, field="as_of")
    normalized = _bounded_json_tree(
        value,
        field=field,
        depth=0,
        counter=[0],
    )
    _validate_public_tree(normalized, field=field)
    _validate_time_bounds(normalized, as_of=boundary, field=field, root=True)
    encoded = canonical_json(normalized, exclude_volatile=False).encode("utf-8")
    if len(encoded) > MAX_FACTOR_ARTIFACT_JSON_BYTES:
        raise ValueError(
            f"{field} exceeds {MAX_FACTOR_ARTIFACT_JSON_BYTES} UTF-8 bytes"
        )
    return _deep_freeze(normalized)


def _unknown_text(value: Any, *, field: str, max_chars: int) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    normalized = value.strip()
    if not normalized or len(normalized) > max_chars:
        raise ValueError(f"{field} must contain 1 to {max_chars} characters")
    if _CONTROL_RE.search(normalized):
        raise ValueError(f"{field} contains control characters")
    _validate_public_tree(normalized, field=field)
    return normalized


def validate_factor_unknowns(
    value: Any,
    *,
    field: str = "unknowns",
    require_canonical: bool = False,
) -> tuple[Mapping[str, str], ...]:
    """Return sorted unique structured unknowns; optionally reject drift."""

    if isinstance(value, (str, bytes, bytearray)) or not isinstance(
        value, Sequence
    ):
        raise TypeError(f"{field} must be an array of mappings")
    if len(value) > MAX_FACTOR_UNKNOWNS:
        raise ValueError(f"{field} exceeds {MAX_FACTOR_UNKNOWNS} items")
    original: list[dict[str, str]] = []
    normalized: list[dict[str, str]] = []
    for index, item in enumerate(value):
        if not isinstance(item, Mapping):
            raise TypeError(f"{field}[{index}] must be a mapping")
        if any(not isinstance(key, str) for key in item):
            raise TypeError(f"{field}[{index}] object keys must be strings")
        keys = set(item)
        if keys != _UNKNOWN_KEYS:
            missing = sorted(_UNKNOWN_KEYS - keys)
            extra = sorted(keys - _UNKNOWN_KEYS)
            detail = []
            if missing:
                detail.append("missing " + ", ".join(missing))
            if extra:
                detail.append("forbidden " + ", ".join(extra))
            raise ValueError(f"{field}[{index}] has invalid fields: {'; '.join(detail)}")
        raw_item = {key: item[key] for key in ("component", "metric", "reason")}
        original.append(raw_item)
        component = _unknown_text(
            raw_item["component"],
            field=f"{field}[{index}].component",
            max_chars=32,
        ).casefold()
        if component not in _FACTOR_COMPONENTS:
            raise ValueError(f"{field}[{index}].component is invalid")
        normalized.append(
            {
                "component": component,
                "metric": _unknown_text(
                    raw_item["metric"],
                    field=f"{field}[{index}].metric",
                    max_chars=128,
                ),
                "reason": _unknown_text(
                    raw_item["reason"],
                    field=f"{field}[{index}].reason",
                    max_chars=256,
                ),
            }
        )

    by_identity = {
        (item["component"], item["metric"], item["reason"]): item
        for item in normalized
    }
    ordered = [by_identity[key] for key in sorted(by_identity)]
    if require_canonical and original != ordered:
        raise ValueError(f"{field} must be sorted, unique, and normalized")
    _validate_public_tree(ordered, field=field)
    return tuple(_deep_freeze(item) for item in ordered)


def validate_factor_dataset_hashes(
    value: Any,
    *,
    field: str = "input_dataset_hashes",
    require_canonical: bool = False,
) -> tuple[str, ...]:
    """Return sorted unique lowercase Dataset hashes or reject bad shape."""

    if isinstance(value, (str, bytes, bytearray)) or not isinstance(
        value, Sequence
    ):
        raise TypeError(f"{field} must be an array")
    if len(value) > MAX_FACTOR_DATASET_HASHES:
        raise ValueError(f"{field} exceeds {MAX_FACTOR_DATASET_HASHES} items")
    original = list(value)
    checked: list[str] = []
    for index, item in enumerate(value):
        digest = require_sha256(item, field=f"{field}[{index}]")
        if item != digest:
            raise ValueError(
                f"{field}[{index}] must be a lowercase SHA-256 digest"
            )
        checked.append(digest)
    normalized = tuple(sorted(set(checked)))
    if require_canonical and original != list(normalized):
        raise ValueError(f"{field} must be sorted and unique")
    return normalized


def _validate_score_projection(
    factor_payload: Mapping[str, Any],
    summaries: Mapping[str, Optional[float]],
) -> None:
    for component_name, summary_field in FACTOR_COMPONENT_SCORE_FIELDS.items():
        component = factor_payload.get(component_name)
        if component is None and component_name not in factor_payload:
            payload_score = None
        else:
            if not isinstance(component, Mapping):
                raise TypeError(f"factor_payload.{component_name} must be a mapping")
            payload_score = _score(
                component.get("score"),
                field=f"factor_payload.{component_name}.score",
            )
        if summaries[summary_field] != payload_score:
            raise ValueError(
                f"{summary_field} does not match "
                f"factor_payload.{component_name}.score"
            )


def validate_factor_contract(
    *,
    factor_payload: Any,
    unknowns: Any,
    input_dataset_hashes: Any,
    as_of: Any,
    available_at: Any,
    value_score: Any = None,
    quality_score: Any = None,
    trend_score: Any = None,
    catalyst_score: Any = None,
    risk_penalty: Any = None,
    require_canonical_references: bool = False,
) -> ValidatedFactorContract:
    """Validate the complete persisted/public Factor field contract.

    ``require_canonical_references=False`` is the writer mode: Dataset hashes
    and structured unknowns are sorted and de-duplicated.  Stored-row and API
    boundaries should pass ``True`` so tampered or drifting representations are
    rejected instead of silently rewritten during egress.
    """

    normalized_as_of = _utc_datetime(as_of, field="as_of")
    normalized_available_at = _utc_datetime(available_at, field="available_at")
    if normalized_available_at > normalized_as_of:
        raise ValueError("available_at cannot be after as_of")
    payload = validate_factor_payload(factor_payload, as_of=normalized_as_of)
    normalized_unknowns = validate_factor_unknowns(
        unknowns,
        require_canonical=require_canonical_references,
    )
    dataset_hashes = validate_factor_dataset_hashes(
        input_dataset_hashes,
        require_canonical=require_canonical_references,
    )
    summaries = {
        "value_score": _score(value_score, field="value_score"),
        "quality_score": _score(quality_score, field="quality_score"),
        "trend_score": _score(trend_score, field="trend_score"),
        "catalyst_score": _score(catalyst_score, field="catalyst_score"),
        "risk_penalty": _score(risk_penalty, field="risk_penalty"),
    }
    _validate_score_projection(payload, summaries)
    return ValidatedFactorContract(
        factor_payload=payload,
        unknowns=normalized_unknowns,
        input_dataset_hashes=dataset_hashes,
        as_of=normalized_as_of,
        available_at=normalized_available_at,
        **summaries,
    )


__all__ = [
    "FACTOR_COMPONENT_SCORE_FIELDS",
    "MAX_FACTOR_ARTIFACT_JSON_BYTES",
    "MAX_FACTOR_DATASET_HASHES",
    "MAX_FACTOR_KEY_CHARS",
    "MAX_FACTOR_MAPPING_ITEMS",
    "MAX_FACTOR_SEQUENCE_ITEMS",
    "MAX_FACTOR_STRING_CHARS",
    "MAX_FACTOR_TREE_DEPTH",
    "MAX_FACTOR_TREE_NODES",
    "MAX_FACTOR_UNKNOWNS",
    "ValidatedFactorContract",
    "validate_factor_contract",
    "validate_factor_dataset_hashes",
    "validate_factor_payload",
    "validate_factor_unknowns",
]
