"""Shared immutable research dataset status contract."""

from __future__ import annotations

from enum import Enum
from typing import Any


class DatasetStatus(str, Enum):
    AVAILABLE = "available"
    EMPTY = "empty"
    PARTIAL = "partial"
    STALE = "stale"
    PERMISSION_DENIED = "permission_denied"
    NOT_SUPPORTED = "not_supported"
    FETCH_FAILED = "fetch_failed"


DATASET_STATUSES = frozenset(status.value for status in DatasetStatus)
PAYLOAD_REQUIRED_STATUSES = frozenset(
    {
        DatasetStatus.AVAILABLE.value,
        DatasetStatus.EMPTY.value,
        DatasetStatus.PARTIAL.value,
        DatasetStatus.STALE.value,
    }
)
PAYLOAD_OPTIONAL_STATUSES = DATASET_STATUSES - PAYLOAD_REQUIRED_STATUSES


def normalize_status(value: DatasetStatus | str) -> str:
    normalized = value.value if isinstance(value, DatasetStatus) else str(value).strip()
    if normalized not in DATASET_STATUSES:
        raise ValueError(f"unsupported research data status: {value!r}")
    return normalized


def validate_dataset_payload(status: DatasetStatus | str, payload: Any) -> str:
    normalized = normalize_status(status)
    if normalized in PAYLOAD_REQUIRED_STATUSES and payload is None:
        raise ValueError(f"status {normalized!r} requires a normalized payload")
    return normalized


__all__ = [
    "DATASET_STATUSES",
    "DatasetStatus",
    "PAYLOAD_OPTIONAL_STATUSES",
    "PAYLOAD_REQUIRED_STATUSES",
    "normalize_status",
    "validate_dataset_payload",
]
