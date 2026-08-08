"""Freeze stable, low-sensitivity research inputs into an immutable snapshot.

This module is deliberately independent of the pipeline, providers, and
storage.  It accepts mappings (or objects exposing safe serialization), emits a
canonical projection, and only imports repository types when persistence is
explicitly requested.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime, time, timezone
import re
from types import MappingProxyType
from typing import Any, Mapping, Optional, Sequence, TYPE_CHECKING
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo

from .canonical import canonical_hash, canonical_json, canonicalize, sha256_hex
from .datasets import normalize_status

if TYPE_CHECKING:
    from .repositories import LeaseFence, ResearchSnapshotInput, ResearchSnapshotRepository, SnapshotWriteResult


SNAPSHOT_VERSION = "research-snapshot-v1"
FIELD_DICTIONARY_VERSION = "research-fields-v1"
FACTOR_ENGINE_VERSION = "factor-engine-v1"

_EXTERNAL_TOKENS = frozenset({"news", "search", "article", "intelligence", "external"})
_CONTENT_FIELDS = frozenset(
    {
        "title",
        "headline",
        "content",
        "body",
        "text",
        "summary",
        "snippet",
        "description",
        "raw_content",
        "news_content",
        "search_result",
    }
)
_REFERENCE_FIELDS = (
    "ref",
    "refs",
    "raw_ref",
    "raw_refs",
    "url",
    "link",
    "source_url",
    "id",
    "item_id",
    "source_id",
    "content_hash",
    "source",
    "provider",
    "available_at",
    "timestamp",
    "provider_timestamp",
    "fetched_at",
    "published_at",
    "announced_at",
    "announcement_date",
)
_EXTERNAL_CONTAINER_FIELDS = ("items", "rows", "data", "results", "articles", "news")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_URL_IN_TEXT_RE = re.compile(r"(?i)\b[a-z][a-z0-9+.-]*://[^\s<>\"']+")
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)\b(token|api[_-]?key|authorization|cookie|password)"
    r"(\s*(?::|=)\s*)(?:bearer\s+)?([^\s,;]+)"
)
_BEARER_RE = re.compile(r"(?i)\bbearer\s+[^\s,;]+")
_KNOWLEDGE_TIME_KEYS = frozenset(
    {
        "available_at",
        "timestamp",
        "provider_timestamp",
        "fetched_at",
        "as_of",
        "data_as_of",
        "published_at",
        "announced_at",
        "announcement_date",
        "ann_date",
        "f_ann_date",
        "imp_ann_date",
        "observed_at",
    }
)
_SENSITIVE_KEY_TOKENS = frozenset(
    {
        "token",
        "api_key",
        "authorization",
        "cookie",
        "password",
        "secret",
        "access_token",
        "refresh_token",
        "client_secret",
        "credential",
        "credentials",
    }
)
_URL_KEY_TOKENS = frozenset({"url", "uri", "link", "endpoint", "base_url", "api_base"})
_ROUTE_STRUCTURAL_FIELDS = frozenset(
    {
        "backend",
        "provider",
        "model",
        "channel",
        "base_url",
        "api_base",
        "fallbacks",
        "fallback_order",
        "fallback_models",
    }
)
_ROUTE_PARAMETER_CONTAINERS = frozenset(
    {"params", "parameters", "model_kwargs", "generation_config"}
)
_A_SHARE_TIMEZONE = ZoneInfo("Asia/Shanghai")


def _required_text(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    return text


def _aware_datetime(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            result = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{field} must be an RFC3339 datetime") from exc
    else:
        raise TypeError(f"{field} must be a timezone-aware datetime or RFC3339 string")
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError(f"{field} must include a timezone offset")
    return result.astimezone(timezone.utc)


def _snake_key(value: Any) -> str:
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", str(value).strip())
    return re.sub(r"[^a-z0-9]+", "_", text.casefold()).strip("_")


def _knowledge_datetime(value: Any, *, field: str) -> datetime:
    """Normalize evidence visibility conservatively.

    Date-only provider fields do not carry a publication time, so they become
    end-of-day in the A-share timezone.  A same-day row is therefore rejected
    unless the snapshot boundary has reached the end of that day.
    """

    if isinstance(value, date) and not isinstance(value, datetime):
        local = datetime.combine(value, time.max, tzinfo=_A_SHARE_TIMEZONE)
        return local.astimezone(timezone.utc)
    if isinstance(value, str):
        text_value = value.strip()
        compact = re.sub(r"\D", "", text_value)
        if len(compact) == 8 and ("T" not in text_value and ":" not in text_value):
            try:
                parsed_date = datetime.strptime(compact, "%Y%m%d").date()
            except ValueError as exc:
                raise ValueError(f"{field} must be a valid knowledge date") from exc
            local = datetime.combine(parsed_date, time.max, tzinfo=_A_SHARE_TIMEZONE)
            return local.astimezone(timezone.utc)
    return _aware_datetime(value, field=field)


def _daily_bar_date_datetime(value: Any, *, field: str) -> datetime:
    """Normalize a typed daily-bar date for future-day rejection.

    Unlike announcement dates, a completed daily bar is observable before the
    end of its trading day.  Date-only daily-bar fields therefore use the
    start of the A-share calendar day: this rejects a later trading date while
    allowing a same-day completed bar after market close.
    """

    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime.combine(value, time.min, tzinfo=_A_SHARE_TIMEZONE).astimezone(
            timezone.utc
        )
    if isinstance(value, str):
        text_value = value.strip()
        compact = re.sub(r"\D", "", text_value)
        if len(compact) == 8 and ("T" not in text_value and ":" not in text_value):
            try:
                parsed_date = datetime.strptime(compact, "%Y%m%d").date()
            except ValueError as exc:
                raise ValueError(f"{field} must be a valid daily-bar date") from exc
            return datetime.combine(
                parsed_date,
                time.min,
                tzinfo=_A_SHARE_TIMEZONE,
            ).astimezone(timezone.utc)
    return _aware_datetime(value, field=field)


def _validate_daily_bar_date(
    value: Any,
    *,
    snapshot_as_of: datetime,
    path: str,
) -> None:
    if value is None or value == "":
        return
    evidence_time = _daily_bar_date_datetime(value, field=path)
    if evidence_time > snapshot_as_of:
        raise ValueError(f"{path} cannot be after snapshot as_of")


def _validate_context_daily_bar_times(
    value: Any,
    *,
    snapshot_as_of: datetime,
) -> None:
    """Validate only typed ContextPack daily-bar dates.

    Generic keys named ``date`` or ``end_date`` are intentionally excluded:
    financial period ends are business dimensions, not publication times.
    """

    if not isinstance(value, Mapping):
        return
    blocks = value.get("blocks")
    from_blocks = isinstance(blocks, Mapping) and isinstance(
        blocks.get("daily_bars"),
        Mapping,
    )
    daily_bars = blocks.get("daily_bars") if from_blocks else None
    if daily_bars is None:
        # Compatibility with the early/legacy ContextPack dictionary shape.
        daily_bars = value.get("daily_bars")
    if not isinstance(daily_bars, Mapping):
        return

    base_path = "context_pack.blocks.daily_bars" if from_blocks else "context_pack.daily_bars"
    metadata = daily_bars.get("metadata")
    if isinstance(metadata, Mapping) and "date" in metadata:
        _validate_daily_bar_date(
            metadata.get("date"),
            snapshot_as_of=snapshot_as_of,
            path=f"{base_path}.metadata.date",
        )

    items = daily_bars.get("items")
    item_container = items if isinstance(items, Mapping) else daily_bars
    items_path = f"{base_path}.items" if isinstance(items, Mapping) else base_path
    for item_name in ("today", "yesterday"):
        item = item_container.get(item_name)
        if not isinstance(item, Mapping):
            continue
        if "date" in item:
            _validate_daily_bar_date(
                item.get("date"),
                snapshot_as_of=snapshot_as_of,
                path=f"{items_path}.{item_name}.date",
            )
        item_metadata = item.get("metadata")
        if isinstance(item_metadata, Mapping) and "date" in item_metadata:
            _validate_daily_bar_date(
                item_metadata.get("date"),
                snapshot_as_of=snapshot_as_of,
                path=f"{items_path}.{item_name}.metadata.date",
            )
        item_value = item.get("value")
        if isinstance(item_value, Mapping) and "date" in item_value:
            _validate_daily_bar_date(
                item_value.get("date"),
                snapshot_as_of=snapshot_as_of,
                path=f"{items_path}.{item_name}.value.date",
            )

    date_item = item_container.get("date")
    if isinstance(date_item, Mapping):
        if "value" in date_item:
            _validate_daily_bar_date(
                date_item.get("value"),
                snapshot_as_of=snapshot_as_of,
                path=f"{items_path}.date.value",
            )
        date_metadata = date_item.get("metadata")
        if isinstance(date_metadata, Mapping) and "date" in date_metadata:
            _validate_daily_bar_date(
                date_metadata.get("date"),
                snapshot_as_of=snapshot_as_of,
                path=f"{items_path}.date.metadata.date",
            )
    elif date_item is not None:
        _validate_daily_bar_date(
            date_item,
            snapshot_as_of=snapshot_as_of,
            path=f"{items_path}.date",
        )


def _is_sensitive_key(value: Any) -> bool:
    normalized = _snake_key(value)
    if normalized in _SENSITIVE_KEY_TOKENS:
        return True
    if normalized.endswith("_token"):
        return True
    tokens = {token for token in normalized.split("_") if token}
    return bool(
        {"password", "secret", "credential", "credentials", "authorization", "cookie"} & tokens
        or ("api" in tokens and "key" in tokens)
        or ("access" in tokens and "token" in tokens)
        or ("refresh" in tokens and "token" in tokens)
    )


def _is_url_key(value: Any) -> bool:
    normalized = _snake_key(value)
    return (
        normalized in _URL_KEY_TOKENS
        or normalized.endswith("_url")
        or normalized.endswith("_uri")
        or normalized.endswith("_endpoint")
    )


def _utc_naive(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _deep_freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_deep_freeze(item) for item in value)
    return value


def _safe_object(value: Any, *, field: str) -> Any:
    if value is None:
        return {}
    safe_serializer = getattr(value, "to_safe_dict", None)
    if callable(safe_serializer):
        return safe_serializer()
    serializer = getattr(value, "to_dict", None)
    if callable(serializer):
        return serializer()
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return model_dump(mode="json")
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, (Mapping, list, tuple)):
        return value
    raise TypeError(f"{field} must be a mapping, sequence, or safely serializable object")


def _is_external_key(key: str) -> bool:
    snake = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", key).casefold()
    tokens = {token for token in re.split(r"[^a-z0-9]+", snake) if token}
    return bool(tokens & _EXTERNAL_TOKENS)


def _sanitize_url(value: Any) -> str:
    text = _required_text(value, "url")
    parsed = urlsplit(text)
    if not parsed.scheme and not parsed.netloc:
        # Host/path configuration forms remain useful for diagnostics, but an
        # opaque reference must never be persisted verbatim: it may itself be
        # a signed path or credential. Only interpret values with an explicit
        # host-shaped first segment as URLs.
        first_segment = re.split(r"[/\\?#]", text.removeprefix("//"), maxsplit=1)[0]
        looks_like_host = (
            text.startswith("//")
            or "." in first_segment
            or ":" in first_segment
            or first_segment.casefold() == "localhost"
        )
        if not looks_like_host:
            opaque = text.split("?", 1)[0].split("#", 1)[0]
            return "[REFERENCE_SHA256:" + sha256_hex(
                f"research-url-opaque-v1\0{opaque}"
            ) + "]"
        parsed = urlsplit(f"//{text.removeprefix('//')}")
        relative = True
    else:
        relative = False
    hostname = parsed.hostname
    if hostname is None:
        opaque = text.split("?", 1)[0].split("#", 1)[0]
        return "[REFERENCE_SHA256:" + sha256_hex(
            f"research-url-opaque-v1\0{opaque}"
        ) + "]"
    host = f"[{hostname}]" if ":" in hostname and not hostname.startswith("[") else hostname
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("base URL contains an invalid port") from exc
    netloc = f"{host}:{port}" if port is not None else host
    path = parsed.path.rstrip("/")
    safe_path = ""
    if path:
        safe_path = "/_path_sha256/" + sha256_hex(
            f"research-url-path-v1\0{path}"
        )
    clean = urlunsplit((parsed.scheme.casefold(), netloc, safe_path, "", ""))
    if relative:
        clean = clean.removeprefix("//")
    return clean


def _project_model_route_url(value: Any) -> Mapping[str, str]:
    """Project a route URL without retaining its credential-bearing path.

    Model gateways sometimes place tenant credentials or signed routing tokens
    in the path rather than in user-info or the query string.  The immutable
    snapshot still needs path changes to affect route identity, but it must not
    persist that path verbatim.  Keep the non-secret origin for diagnostics and
    bind the complete sanitized path through a one-way fingerprint.
    """

    clean = _sanitize_url(value)
    if clean.startswith("[REFERENCE_SHA256:"):
        return {
            "reference_fingerprint": sha256_hex(
                f"model-route-opaque-reference-v1\0{clean}"
            )
        }
    parsed = urlsplit(clean)
    if not parsed.scheme and not parsed.netloc:
        parsed = urlsplit(f"//{clean}")

    hostname = parsed.hostname
    if hostname is None:
        return {
            "reference_fingerprint": sha256_hex(
                f"model-route-opaque-reference-v1\0{clean}"
            )
        }

    host = f"[{hostname}]" if ":" in hostname and not hostname.startswith("[") else hostname
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("base URL contains an invalid port") from exc
    netloc = f"{host}:{port}" if port is not None else host
    origin = urlunsplit((parsed.scheme.casefold(), netloc, "", "", ""))
    if not parsed.scheme:
        origin = origin.removeprefix("//")

    projected = {"origin": origin}
    path = parsed.path.rstrip("/")
    if path:
        projected["path_fingerprint"] = sha256_hex(
            f"model-route-path-v1\0{path}"
        )
    return projected


def _sanitize_text_urls(value: str) -> str:
    def replace(match: re.Match[str]) -> str:
        raw = match.group(0)
        trailing = ""
        while raw and raw[-1] in ".,;)]}":
            trailing = raw[-1] + trailing
            raw = raw[:-1]
        return _sanitize_url(raw) + trailing

    sanitized = _URL_IN_TEXT_RE.sub(replace, value)
    sanitized = _SECRET_ASSIGNMENT_RE.sub(
        lambda match: f"{match.group(1)}{match.group(2)}[REDACTED]",
        sanitized,
    )
    return _BEARER_RE.sub("Bearer [REDACTED]", sanitized)


def _sanitize_tree(value: Any) -> Any:
    """Drop secret-bearing fields and sanitize every URL-shaped field."""

    if isinstance(value, Mapping):
        projected: dict[str, Any] = {}
        for raw_key, item in value.items():
            key = str(raw_key)
            if _is_sensitive_key(key):
                continue
            if _is_url_key(key) and isinstance(item, str) and item.strip():
                projected[key] = _sanitize_url(item)
            else:
                projected[key] = _sanitize_tree(item)
        return projected
    if isinstance(value, (list, tuple)):
        return [_sanitize_tree(item) for item in value]
    if isinstance(value, str):
        return _sanitize_text_urls(value)
    return value


def _sanitize_model_route_tree(value: Any) -> Any:
    """Sanitize nested route parameters while fingerprinting URL paths."""

    if isinstance(value, Mapping):
        projected: dict[str, Any] = {}
        for raw_key, item in value.items():
            key = str(raw_key)
            if _is_sensitive_key(key):
                continue
            if _is_url_key(key) and isinstance(item, str) and item.strip():
                projected[key] = _project_model_route_url(item)
            else:
                projected[key] = _sanitize_model_route_tree(item)
        return projected
    if isinstance(value, (list, tuple)):
        return [_sanitize_model_route_tree(item) for item in value]
    if isinstance(value, str):
        return _sanitize_text_urls(value)
    return value


def _safe_reference(value: Any) -> Any:
    if isinstance(value, str):
        return _sanitize_text_urls(value)
    if isinstance(value, Mapping):
        projected: dict[str, Any] = {}
        for key in (
            "id",
            "item_id",
            "source_id",
            "hash",
            "content_hash",
            "url",
            "link",
            "source_url",
            "raw_ref",
            "source",
            "provider",
            "available_at",
            "timestamp",
            "provider_timestamp",
            "fetched_at",
            "published_at",
            "announced_at",
            "announcement_date",
        ):
            if key not in value:
                continue
            item = value[key]
            if key in {"url", "link", "source_url"} and item is not None:
                projected[key] = _sanitize_url(item)
            elif key == "raw_ref" and isinstance(item, Mapping):
                projected[key] = _safe_reference(item)
            else:
                projected[key] = canonicalize(item)
        return projected
    if isinstance(value, (list, tuple)):
        return [_safe_reference(item) for item in value]
    return canonicalize(value)


def _external_record_projection(record: Any) -> Mapping[str, Any]:
    canonical_record = canonicalize(_sanitize_tree(record))
    if isinstance(canonical_record, Mapping):
        content = {
            key: item
            for key, item in canonical_record.items()
            if key.casefold() in _CONTENT_FIELDS
        }
        fingerprint_source = content if content else canonical_record
        content_text = canonical_json(fingerprint_source, exclude_volatile=False)
        refs = []
        for key in _REFERENCE_FIELDS:
            if key in canonical_record:
                refs.append({key: _safe_reference(canonical_record[key])})
    else:
        content_text = canonical_json(canonical_record, exclude_volatile=False)
        refs = []
    return {
        "content_hash": sha256_hex(content_text),
        "utf8_length": len(content_text.encode("utf-8")),
        "refs": refs,
    }


def _project_external(value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        return [_project_external(item) for item in value]
    if isinstance(value, Mapping):
        # Preserve container shape/status while every external record is reduced
        # to a hash, byte length, and references.
        for container_key in _EXTERNAL_CONTAINER_FIELDS:
            if container_key in value:
                projected: dict[str, Any] = {}
                for key in (
                    "status",
                    "source",
                    "provider",
                    "as_of",
                    "data_as_of",
                    "available_at",
                    "timestamp",
                    "provider_timestamp",
                    "fetched_at",
                    "published_at",
                    "announced_at",
                    "announcement_date",
                ):
                    if key in value:
                        projected[key] = _safe_reference(value[key])
                projected[container_key] = _project_external(value[container_key])
                return projected
        if "value" in value and set(value).intersection({"status", "source", "timestamp", "metadata"}):
            projected = {}
            for key in (
                "status",
                "source",
                "provider",
                "as_of",
                "available_at",
                "timestamp",
                "provider_timestamp",
                "fetched_at",
                "published_at",
                "announced_at",
                "announcement_date",
            ):
                if key in value:
                    projected[key] = _safe_reference(value[key])
            projected["value"] = _project_external(value["value"])
            return projected
    return _external_record_projection(value)


def _project_tree(value: Any, *, external: bool = False) -> Any:
    if external:
        return _project_external(value)
    if isinstance(value, Mapping):
        dataset_kind = " ".join(
            str(value.get(key) or "") for key in ("dataset", "kind", "type", "source_type")
        )
        if _is_external_key(dataset_kind):
            projected: dict[str, Any] = {}
            for key in ("dataset", "kind", "type", "source_type", "status", "as_of", "data_as_of", "available_at"):
                if key in value:
                    projected[key] = canonicalize(value[key])
            payload_found = False
            for key in ("normalized", "payload", "items", "rows", "data", "results", "articles", "news"):
                if key in value:
                    projected[key] = _project_external(value[key])
                    payload_found = True
            if not payload_found:
                projected["content"] = _external_record_projection(value)
            return projected
        return {
            str(key): (
                _sanitize_url(item)
                if _is_url_key(str(key)) and isinstance(item, str) and item.strip()
                else _project_tree(item, external=_is_external_key(str(key)))
            )
            for key, item in value.items()
            if not _is_sensitive_key(str(key))
        }
    if isinstance(value, (list, tuple)):
        return [_project_tree(item) for item in value]
    if isinstance(value, str):
        return _sanitize_text_urls(value)
    return value


def _validate_evidence_times(value: Any, *, snapshot_as_of: datetime, path: str = "payload") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = _snake_key(key)
            if normalized in _KNOWLEDGE_TIME_KEYS and item is not None:
                evidence_time = _knowledge_datetime(item, field=f"{path}.{key}")
                if evidence_time > snapshot_as_of:
                    raise ValueError(f"{path}.{key} cannot be after snapshot as_of")
            _validate_evidence_times(item, snapshot_as_of=snapshot_as_of, path=f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _validate_evidence_times(item, snapshot_as_of=snapshot_as_of, path=f"{path}[{index}]")


def safe_project_context_pack(context_pack: Any, *, as_of: Any) -> Any:
    cutoff = _aware_datetime(as_of, field="as_of")
    safe = _safe_object(context_pack, field="context_pack")
    _validate_context_daily_bar_times(safe, snapshot_as_of=cutoff)
    _validate_evidence_times(safe, snapshot_as_of=cutoff, path="context_pack")
    return canonicalize(_project_tree(safe))


def project_structured_datasets(datasets: Any, *, as_of: Any) -> Any:
    cutoff = _aware_datetime(as_of, field="as_of")
    safe = _safe_object(datasets, field="datasets")
    _validate_evidence_times(safe, snapshot_as_of=cutoff, path="datasets")
    return canonicalize(_project_tree(safe))


def project_factors(factors: Any, *, as_of: Any) -> Any:
    cutoff = _aware_datetime(as_of, field="as_of")
    if factors is None:
        return None
    safe = _safe_object(factors, field="factors")
    _validate_evidence_times(safe, snapshot_as_of=cutoff, path="factors")
    return canonicalize(_project_tree(safe))


def _route_output_parameters(route: Mapping[str, Any]) -> Mapping[str, Any]:
    parameter_sources = [route]
    for container_key in sorted(_ROUTE_PARAMETER_CONTAINERS):
        candidate = route.get(container_key)
        if candidate is not None:
            if not isinstance(candidate, Mapping):
                raise TypeError(f"model_route.{container_key} must be a mapping")
            parameter_sources.append(candidate)
    projected: dict[str, Any] = {}
    for source in parameter_sources:
        ordered_keys = sorted(
            source,
            key=lambda key: (
                "deployment" if _snake_key(key) == "azure_deployment" else _snake_key(key),
                _snake_key(key) != "deployment",
                str(key),
            ),
        )
        for raw_key in ordered_keys:
            normalized_key = _snake_key(raw_key)
            if not normalized_key or _is_sensitive_key(normalized_key):
                continue
            if (
                normalized_key in _ROUTE_STRUCTURAL_FIELDS
                or normalized_key in _ROUTE_PARAMETER_CONTAINERS
            ):
                continue
            target_key = "deployment" if normalized_key == "azure_deployment" else normalized_key
            if target_key in projected:
                continue
            # canonicalize with volatile filtering so request/job/trace timing
            # metadata remains outside the model-route identity while every
            # non-secret generation knob (including future ones) is retained.
            candidate = canonicalize(
                _sanitize_model_route_tree({target_key: source[raw_key]})
            )
            if target_key in candidate:
                projected[target_key] = candidate[target_key]
    return projected


def project_model_route(route: Mapping[str, Any]) -> Mapping[str, Any]:
    if not isinstance(route, Mapping):
        raise TypeError("model_route must be a mapping")
    backend = route.get("backend", route.get("provider"))
    base_url = route.get("base_url", route.get("api_base"))
    projected: dict[str, Any] = {
        "backend": _required_text(backend, "model_route.backend"),
        "model": _required_text(route.get("model"), "model_route.model"),
        "channel": _required_text(route.get("channel"), "model_route.channel"),
    }
    if base_url is not None:
        projected["base_url"] = _project_model_route_url(base_url)
    projected.update(_route_output_parameters(route))
    raw_fallbacks = route.get("fallbacks", route.get("fallback_order", route.get("fallback_models", ())))
    if raw_fallbacks is None:
        raw_fallbacks = ()
    if not isinstance(raw_fallbacks, Sequence) or isinstance(raw_fallbacks, (str, bytes, bytearray)):
        raise TypeError("model_route fallbacks must be an ordered sequence")
    fallbacks = []
    for index, fallback in enumerate(raw_fallbacks):
        if isinstance(fallback, str):
            fallbacks.append({"model": _required_text(fallback, f"fallbacks[{index}]")})
            continue
        if not isinstance(fallback, Mapping):
            raise TypeError("each model fallback must be a string or mapping")
        item: dict[str, Any] = {}
        fallback_backend = fallback.get("backend", fallback.get("provider"))
        fallback_base_url = fallback.get("base_url", fallback.get("api_base"))
        if fallback_backend is not None:
            item["backend"] = _required_text(fallback_backend, f"fallbacks[{index}].backend")
        if fallback.get("model") is not None:
            item["model"] = _required_text(fallback["model"], f"fallbacks[{index}].model")
        if fallback.get("channel") is not None:
            item["channel"] = _required_text(fallback["channel"], f"fallbacks[{index}].channel")
        if fallback_base_url is not None:
            item["base_url"] = _project_model_route_url(fallback_base_url)
        item.update(_route_output_parameters(fallback))
        if "model" not in item and "deployment" not in item:
            raise ValueError(f"fallbacks[{index}] requires model or deployment")
        fallbacks.append(item)
    projected["fallbacks"] = fallbacks
    return canonicalize(projected, exclude_volatile=False)


def model_route_fingerprint(route: Mapping[str, Any]) -> str:
    return canonical_hash(project_model_route(route), exclude_volatile=False)


@dataclass(frozen=True)
class FrozenResearchSnapshot:
    stock_code: str
    market: str
    snapshot_version: str
    field_dictionary_version: str
    factor_engine_version: str
    pack_version: str
    prompt_version: str
    policy_version: str
    model_route_fingerprint: str
    as_of: datetime
    available_at: datetime
    status: str
    canonical_payload: Mapping[str, Any]
    canonical_json: str
    snapshot_hash: str
    factor_snapshot_hash: Optional[str] = None

    def to_repository_input(self) -> "ResearchSnapshotInput":
        from .repositories import ResearchSnapshotInput

        return ResearchSnapshotInput(
            stock_code=self.stock_code,
            market=self.market,
            snapshot_version=self.snapshot_version,
            field_dictionary_version=self.field_dictionary_version,
            factor_engine_version=self.factor_engine_version,
            pack_version=self.pack_version,
            prompt_version=self.prompt_version,
            policy_version=self.policy_version,
            model_route_fingerprint=self.model_route_fingerprint,
            as_of=self.as_of,
            available_at=self.available_at,
            status=self.status,
            canonical_payload=self.canonical_payload,
            factor_snapshot_hash=self.factor_snapshot_hash,
        )

    def persist(
        self,
        repository: "ResearchSnapshotRepository",
        *,
        lease: "LeaseFence",
        now: Optional[datetime] = None,
    ) -> "SnapshotWriteResult":
        return repository.write_research_snapshot(self.to_repository_input(), lease=lease, now=now)


def build_research_snapshot(
    *,
    stock_code: str,
    market: str,
    as_of: Any,
    available_at: Any,
    context_pack: Any,
    datasets: Any,
    factors: Any,
    prompt_version: str,
    prompt: Any,
    model_route: Mapping[str, Any],
    policy_version: str,
    policy: Any,
    pack_version: str,
    status: str = "available",
    snapshot_version: str = SNAPSHOT_VERSION,
    field_dictionary_version: str = FIELD_DICTIONARY_VERSION,
    factor_engine_version: str = FACTOR_ENGINE_VERSION,
    factor_snapshot_hash: Optional[str] = None,
) -> FrozenResearchSnapshot:
    cutoff = _aware_datetime(as_of, field="as_of")
    observable_at = _aware_datetime(available_at, field="available_at")
    if observable_at > cutoff:
        raise ValueError("available_at cannot be after as_of")
    if factor_snapshot_hash is not None and not _SHA256_RE.fullmatch(str(factor_snapshot_hash)):
        raise ValueError("factor_snapshot_hash must be a lowercase SHA-256 digest")
    route_fingerprint = model_route_fingerprint(model_route)
    payload = canonicalize(
        {
            "context_pack": safe_project_context_pack(context_pack, as_of=cutoff),
            "datasets": project_structured_datasets(datasets, as_of=cutoff),
            "factors": project_factors(factors, as_of=cutoff),
            # Prompt and policy bodies are not retained.  Their fingerprints
            # make semantic changes alter the immutable snapshot identity.
            "prompt_fingerprint": canonical_hash(prompt),
            "policy_fingerprint": canonical_hash(policy),
        }
    )
    values = {
        "stock_code": _required_text(stock_code, "stock_code"),
        "market": _required_text(market, "market"),
        "snapshot_version": _required_text(snapshot_version, "snapshot_version"),
        "field_dictionary_version": _required_text(field_dictionary_version, "field_dictionary_version"),
        "factor_engine_version": _required_text(factor_engine_version, "factor_engine_version"),
        "pack_version": _required_text(pack_version, "pack_version"),
        "prompt_version": _required_text(prompt_version, "prompt_version"),
        "policy_version": _required_text(policy_version, "policy_version"),
        "model_route_fingerprint": route_fingerprint,
        "as_of": _utc_naive(cutoff),
        "available_at": _utc_naive(observable_at),
        "status": normalize_status(status),
        "canonical_json": payload,
        "factor_snapshot_hash": factor_snapshot_hash,
    }
    snapshot_hash = canonical_hash(values)
    return FrozenResearchSnapshot(
        stock_code=values["stock_code"],
        market=values["market"],
        snapshot_version=values["snapshot_version"],
        field_dictionary_version=values["field_dictionary_version"],
        factor_engine_version=values["factor_engine_version"],
        pack_version=values["pack_version"],
        prompt_version=values["prompt_version"],
        policy_version=values["policy_version"],
        model_route_fingerprint=route_fingerprint,
        as_of=cutoff,
        available_at=observable_at,
        status=values["status"],
        canonical_payload=_deep_freeze(payload),
        canonical_json=canonical_json(payload),
        snapshot_hash=snapshot_hash,
        factor_snapshot_hash=factor_snapshot_hash,
    )


def persist_research_snapshot(
    snapshot: FrozenResearchSnapshot,
    repository: "ResearchSnapshotRepository",
    *,
    lease: "LeaseFence",
    now: Optional[datetime] = None,
) -> "SnapshotWriteResult":
    return snapshot.persist(repository, lease=lease, now=now)


__all__ = [
    "FACTOR_ENGINE_VERSION",
    "FIELD_DICTIONARY_VERSION",
    "FrozenResearchSnapshot",
    "SNAPSHOT_VERSION",
    "build_research_snapshot",
    "model_route_fingerprint",
    "persist_research_snapshot",
    "project_factors",
    "project_model_route",
    "project_structured_datasets",
    "safe_project_context_pack",
]
