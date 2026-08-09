"""Strict, bounded validation helpers for research Debate artifacts.

Debate output is model-generated interpretation, not evidence.  This module
keeps that interpretation small, reference-only, and free of executable trade
instructions or raw source URLs before it can enter an immutable artifact or a
later prompt.
"""

from __future__ import annotations

import math
import re
from typing import Any, Iterable, Mapping

from .evidence_security import require_identifier, require_sha256


MAX_DEBATE_ARGUMENTS_PER_STANCE = 6
MAX_DEBATE_CLAIM_IDS_PER_ARGUMENT = 8
MAX_DEBATE_CITATION_IDS_PER_ARGUMENT = 8
MAX_DEBATE_LIMITATIONS_PER_ARGUMENT = 4
MAX_DEBATE_OPEN_QUESTIONS_PER_STANCE = 6
MAX_DEBATE_SUMMARY_CHARS = 1_000
MAX_DEBATE_STATEMENT_CHARS = 1_000
MAX_DEBATE_LIMITATION_CHARS = 300
MAX_DEBATE_OPEN_QUESTION_CHARS = 500
MAX_DEBATE_MESSAGE_CHARS = 18_000
MAX_DEBATE_REQUEST_CHARS = 36_000
MAX_DEBATE_OUTPUT_CHARS = 64_000
MAX_DEBATE_ARTIFACT_JSON_CHARS = 256_000
MAX_DEBATE_PROMPT_CONTEXT_CHARS = 12_000

_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_RAW_URL_RE = re.compile(r"(?i)(?:[a-z][a-z0-9+.-]*://|www\.)\S+")
_TOKEN_LIKE_RE = re.compile(
    r"(?i)\b(?:sk-[a-z0-9_-]{16,}|xox[baprs]-[a-z0-9-]{16,}|"
    r"gh[pousr]_[a-z0-9_]{20,})\b"
)
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?ix)\b(?:token|api[_-]?key|access[_-]?token|refresh[_-]?token|"
    r"password|passwd|secret|client[_-]?secret|private[_-]?key|"
    r"authorization|cookie)\b\s*(?::|=)\s*"
    r"(?![\"']?\[REDACTED\][\"']?)\S+"
)
_BEARER_RE = re.compile(r"(?i)\bbearer\s+(?!\[REDACTED\])\S+")
_MODEL_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/+:-]{0,127}$")
_SCHEME_PATH_RE = re.compile(r"(?i)^[a-z][a-z0-9+.-]*:/{1,2}")
# Keep the policy in ASCII source form so Windows checkout encodings cannot
# corrupt the Chinese action terms.
_DECISION_DIRECTIVE_RE = re.compile(
    r"(?ix)"
    r"\b(?:final\s+recommendation|price\s+target|target\s+price|"
    r"position\s+size|entry\s+price|take\s+profit|stop\s+loss|"
    r"operation[_\s-]?advice|recommend(?:ation|ed)?\s+(?:buy|sell|hold))\b"
    r"|\b(?:buy|sell|hold|bought|sold|overweight|underweight)\b"
    r"|\b(?:buying|selling|holding)\b"
    r"|\b(?:go|stay|remain)\s+(?:long|short)\b"
    r"|\b(?:long|short)\s+position\b"
    r"|(?:\u6700\u7ec8\u5efa\u8bae|\u64cd\u4f5c\u5efa\u8bae|"
    r"\u4ed3\u4f4d\u5efa\u8bae|\u76ee\u6807\u4ef7|"
    r"\u5efa\u8bae(?:\u4e70\u5165|\u5356\u51fa|\u6301\u6709)|"
    r"\u5165\u573a\u4ef7|\u6b62\u635f\u4ef7|\u6b62\u76c8\u4ef7|"
    r"\u52a0\u4ed3|\u51cf\u4ed3|\u6e05\u4ed3|\u5efa\u4ed3)"
)


def strict_bounded_text(
    value: Any,
    *,
    field: str,
    max_chars: int,
    required: bool = True,
) -> str:
    """Normalize harmless line endings while rejecting truncation ambiguity."""

    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    normalized = value.replace("\r\n", "\n").replace("\r", "\n").strip()
    if _CONTROL_RE.search(normalized):
        raise ValueError(f"{field} contains forbidden control characters")
    if required and not normalized:
        raise ValueError(f"{field} is required")
    if len(normalized) > max_chars:
        raise ValueError(f"{field} exceeds {max_chars} characters")
    return normalized


def strict_identifier(value: Any, *, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    return require_identifier(value, field=field)


def strict_fingerprint(value: Any, *, field: str) -> str:
    return require_sha256(value, field=field)


def strict_confidence(value: Any, *, field: str = "confidence") -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field} must be a number")
    normalized = float(value)
    if not math.isfinite(normalized) or not 0.0 <= normalized <= 1.0:
        raise ValueError(f"{field} must be finite and between zero and one")
    return normalized


def strict_identifier_list(
    values: Any,
    *,
    field: str,
    maximum: int,
    minimum: int = 1,
) -> tuple[str, ...]:
    if isinstance(values, (str, bytes, bytearray)) or not isinstance(
        values, (list, tuple)
    ):
        raise TypeError(f"{field} must be an array")
    if not minimum <= len(values) <= maximum:
        raise ValueError(f"{field} must contain between {minimum} and {maximum} items")
    normalized = tuple(
        strict_identifier(item, field=f"{field}[{index}]")
        for index, item in enumerate(values)
    )
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field} must contain unique identifiers")
    return tuple(sorted(normalized))


def strict_text_list(
    values: Any,
    *,
    field: str,
    maximum: int,
    max_chars: int,
) -> tuple[str, ...]:
    if isinstance(values, (str, bytes, bytearray)) or not isinstance(
        values, (list, tuple)
    ):
        raise TypeError(f"{field} must be an array")
    if len(values) > maximum:
        raise ValueError(f"{field} exceeds {maximum} items")
    normalized = tuple(
        strict_bounded_text(
            item,
            field=f"{field}[{index}]",
            max_chars=max_chars,
        )
        for index, item in enumerate(values)
    )
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field} must contain unique values")
    return normalized


def require_exact_keys(
    value: Any,
    *,
    field: str,
    required: Iterable[str],
    optional: Iterable[str] = (),
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field} must be an object")
    required_keys = frozenset(required)
    allowed_keys = required_keys | frozenset(optional)
    keys = {str(key) for key in value}
    missing = sorted(required_keys - keys)
    extra = sorted(keys - allowed_keys)
    if missing:
        raise ValueError(f"{field} is missing required fields: {', '.join(missing)}")
    if extra:
        raise ValueError(f"{field} contains forbidden fields: {', '.join(extra)}")
    return value


def validate_secret_safe_text(value: str, *, field: str) -> None:
    if (
        _TOKEN_LIKE_RE.search(value)
        or _SECRET_ASSIGNMENT_RE.search(value)
        or _BEARER_RE.search(value)
    ):
        raise ValueError(f"{field} contains secret-like material")


def strict_public_identifier(value: Any, *, field: str) -> str:
    """Return an externally visible identifier that cannot carry secrets."""

    normalized = strict_identifier(value, field=field)
    validate_secret_safe_text(normalized, field=field)
    return normalized


def strict_version_identifier(value: Any, *, field: str) -> str:
    """Return one bounded, display-safe Debate contract version identifier."""

    bounded = strict_bounded_text(
        value,
        field=field,
        max_chars=64,
    )
    return strict_public_identifier(bounded, field=field)


def strict_error_code(value: Any, *, field: str = "error_code") -> str:
    """Return a bounded error identifier that cannot itself carry credentials."""

    bounded = strict_bounded_text(
        value,
        field=field,
        max_chars=64,
    )
    return strict_public_identifier(bounded, field=field)


def strict_model_identifier(value: Any, *, field: str = "model_used") -> str:
    """Accept a display-safe provider/model label, never a raw route or URL."""

    normalized = strict_bounded_text(
        value,
        field=field,
        max_chars=128,
    )
    validate_secret_safe_text(normalized, field=field)
    if (
        _RAW_URL_RE.search(normalized)
        or _SCHEME_PATH_RE.search(normalized)
        or not _MODEL_IDENTIFIER_RE.fullmatch(normalized)
    ):
        raise ValueError(f"{field} must be a safe model identifier")
    return normalized


def validate_debate_prose(value: str, *, field: str) -> None:
    """Reject source leakage and decision/thesis semantics from Debate prose."""

    validate_secret_safe_text(value, field=field)
    if _RAW_URL_RE.search(value):
        raise ValueError(f"{field} must reference citation IDs instead of raw URLs")
    if _DECISION_DIRECTIVE_RE.search(value):
        raise ValueError(f"{field} contains forbidden decision or target language")


__all__ = [
    "MAX_DEBATE_ARGUMENTS_PER_STANCE",
    "MAX_DEBATE_CITATION_IDS_PER_ARGUMENT",
    "MAX_DEBATE_CLAIM_IDS_PER_ARGUMENT",
    "MAX_DEBATE_LIMITATIONS_PER_ARGUMENT",
    "MAX_DEBATE_LIMITATION_CHARS",
    "MAX_DEBATE_MESSAGE_CHARS",
    "MAX_DEBATE_OPEN_QUESTIONS_PER_STANCE",
    "MAX_DEBATE_OPEN_QUESTION_CHARS",
    "MAX_DEBATE_OUTPUT_CHARS",
    "MAX_DEBATE_ARTIFACT_JSON_CHARS",
    "MAX_DEBATE_PROMPT_CONTEXT_CHARS",
    "MAX_DEBATE_REQUEST_CHARS",
    "MAX_DEBATE_STATEMENT_CHARS",
    "MAX_DEBATE_SUMMARY_CHARS",
    "require_exact_keys",
    "strict_bounded_text",
    "strict_confidence",
    "strict_error_code",
    "strict_fingerprint",
    "strict_identifier",
    "strict_identifier_list",
    "strict_model_identifier",
    "strict_public_identifier",
    "strict_version_identifier",
    "strict_text_list",
    "validate_debate_prose",
    "validate_secret_safe_text",
]
