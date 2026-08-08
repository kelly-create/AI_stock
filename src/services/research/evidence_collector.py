"""Provider-agnostic, snippet-only evidence search collection.

Live mode performs at most one injected search call and never dereferences a
result URL.  Historical mode is replay-only.  The normalized response can be
stored as the generic ``news_search`` research dataset before the evidence DAG
references its immutable content hash.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, time, timezone
import re
from types import MappingProxyType
from typing import Any, Callable, Mapping, Optional, Sequence, TYPE_CHECKING
from zoneinfo import ZoneInfo

from .canonical import canonical_hash, canonicalize
from .datasets import DatasetStatus
from .evidence_security import (
    MAX_EVIDENCE_EXCERPT_CHARS,
    MAX_EVIDENCE_TITLE_CHARS,
    bounded_text,
    json_value_hash,
    require_aware_utc,
    require_sha256,
    safe_canonical_url,
)
from .evidence_service import EvidenceArtifact, EvidenceCitation, ResearchClaim

if TYPE_CHECKING:
    from .repositories import DatasetSnapshotInput, LeaseFence, ResearchSnapshotRepository


NEWS_SEARCH_DATASET = "news_search"
NEWS_SEARCH_SCHEMA_VERSION = "news-search-snippet-v1"
MAX_LIVE_SEARCH_RESULTS = 5
_A_SHARE_TIMEZONE = ZoneInfo("Asia/Shanghai")
_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

_COLLECTION_STATUSES = frozenset(
    {
        DatasetStatus.AVAILABLE.value,
        DatasetStatus.EMPTY.value,
        DatasetStatus.PARTIAL.value,
        DatasetStatus.FETCH_FAILED.value,
    }
)


class EvidenceCollectionCancelledError(RuntimeError):
    """Raised at a safe boundary when the durable job has been cancelled."""


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _deep_freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_deep_freeze(item) for item in value)
    return value


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _cancelled(cancel_event: Any) -> bool:
    if cancel_event is None:
        return False
    check = getattr(cancel_event, "is_set", None)
    if not callable(check):
        raise TypeError("cancel_event must expose is_set()")
    return bool(check())


def _raise_if_cancelled(cancel_event: Any) -> None:
    if _cancelled(cancel_event):
        raise EvidenceCollectionCancelledError("research evidence collection cancelled")


def _safe_error(value: Any) -> Optional[str]:
    if value is None:
        return None
    # Reuse the PR2 storage-boundary sanitizer so credentials and URL paths are
    # handled identically across dataset and evidence failures.
    from .repositories import sanitize_error_message

    return sanitize_error_message(value)


def _identifier(prefix: str, payload: Any) -> str:
    return f"{prefix}_{canonical_hash(payload, exclude_volatile=False)[:24]}"


def _publication_time(value: Any, *, boundary: datetime) -> datetime:
    if value is None or not str(value).strip():
        raise ValueError("search_result published time is required")
    published_text = str(value).strip()
    if _DATE_ONLY_RE.fullmatch(published_text):
        try:
            publication_date = date.fromisoformat(published_text)
        except ValueError as exc:
            raise ValueError("search_result.published_at is invalid") from exc
        # A date without a release time becomes knowable only at the most
        # conservative end-of-day boundary in the A-share market timezone.
        published_at = datetime.combine(
            publication_date,
            time.max,
            tzinfo=_A_SHARE_TIMEZONE,
        ).astimezone(timezone.utc)
    else:
        published_at = require_aware_utc(value, field="search_result.published_at")
    if published_at > boundary:
        raise ValueError("search_result.published_at cannot be after as_of")
    return published_at


def _normalized_items(
    normalized: Mapping[str, Any],
    *,
    as_of: datetime,
    available_at: datetime,
) -> tuple[Mapping[str, Any], ...]:
    raw_items = normalized.get("items", ())
    if not isinstance(raw_items, Sequence) or isinstance(raw_items, (str, bytes, bytearray)):
        raise TypeError("news_search normalized.items must be a sequence")
    if len(raw_items) > MAX_LIVE_SEARCH_RESULTS:
        raise ValueError(f"news_search items exceed {MAX_LIVE_SEARCH_RESULTS}")
    items: list[Mapping[str, Any]] = []
    for index, item in enumerate(raw_items):
        if not isinstance(item, Mapping):
            raise TypeError(f"news_search items[{index}] must be a mapping")
        # Only the snippet contract is accepted.  Full bodies/content are not
        # copied even if a provider adds them to the object.
        item_available_at = require_aware_utc(
            item.get("available_at"),
            field=f"news_search items[{index}].available_at",
        )
        if item_available_at != available_at or item_available_at > as_of:
            raise ValueError("news_search item availability conflicts with its collection")
        allowed = {
            "title": bounded_text(
                item.get("title"),
                field=f"news_search items[{index}].title",
                max_chars=MAX_EVIDENCE_TITLE_CHARS,
                required=True,
            ),
            "snippet": bounded_text(
                item.get("snippet"),
                field=f"news_search items[{index}].snippet",
                max_chars=MAX_EVIDENCE_EXCERPT_CHARS,
                required=True,
            ),
            "source": bounded_text(
                item.get("source"),
                field=f"news_search items[{index}].source",
                max_chars=160,
                required=True,
            ),
            "published_at": _publication_time(
                item.get("published_at"),
                boundary=available_at,
            ),
            "available_at": item_available_at,
            "canonical_url": safe_canonical_url(
                item.get("canonical_url"),
                field=f"news_search items[{index}].canonical_url",
            ),
        }
        items.append(
            MappingProxyType(canonicalize(allowed, exclude_volatile=False))
        )
    return tuple(items)


@dataclass(frozen=True)
class EvidenceCollectionResult:
    stock_code: str
    as_of: datetime
    observed_at: datetime
    status: str
    provider: str
    normalized: Mapping[str, Any]
    available_at: datetime
    content_hash: Optional[str] = None
    raw_ref: Optional[Mapping[str, Any]] = None
    error_code: Optional[str] = None
    error_message_sanitized: Optional[str] = None
    searched: bool = False
    limitations: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        stock_code = bounded_text(
            self.stock_code,
            field="stock_code",
            max_chars=64,
            required=True,
        )
        object.__setattr__(self, "stock_code", stock_code)
        boundary = require_aware_utc(self.as_of, field="as_of")
        observed_at = require_aware_utc(self.observed_at, field="observed_at")
        available_at = require_aware_utc(self.available_at, field="available_at")
        if observed_at > boundary or available_at > boundary:
            raise ValueError("evidence collection cannot cross its as_of boundary")
        object.__setattr__(self, "as_of", boundary)
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "available_at", available_at)
        status = str(self.status or "").strip().casefold()
        if status not in _COLLECTION_STATUSES:
            raise ValueError(f"unsupported evidence collection status: {self.status!r}")
        object.__setattr__(self, "status", status)
        object.__setattr__(
            self,
            "provider",
            bounded_text(
                self.provider,
                field="provider",
                max_chars=128,
                required=True,
            ),
        )
        if not isinstance(self.normalized, Mapping):
            raise TypeError("normalized must be a mapping")
        normalized = canonicalize(self.normalized, exclude_volatile=False)
        if normalized.get("schema_version") != NEWS_SEARCH_SCHEMA_VERSION:
            raise ValueError("news_search normalized schema_version is invalid")
        items = _normalized_items(
            normalized,
            as_of=boundary,
            available_at=available_at,
        )
        normalized["items"] = [_plain(item) for item in items]
        object.__setattr__(self, "normalized", _deep_freeze(normalized))
        if status == DatasetStatus.AVAILABLE.value and not items:
            raise ValueError("available news_search collection requires items")
        if status == DatasetStatus.EMPTY.value and items:
            raise ValueError("empty news_search collection cannot contain items")
        if self.content_hash is not None:
            object.__setattr__(
                self,
                "content_hash",
                require_sha256(self.content_hash, field="content_hash"),
            )
        if self.raw_ref is not None:
            if not isinstance(self.raw_ref, Mapping):
                raise TypeError("raw_ref must be a mapping")
            object.__setattr__(
                self,
                "raw_ref",
                _deep_freeze(canonicalize(self.raw_ref, exclude_volatile=False)),
            )
        object.__setattr__(
            self,
            "error_code",
            bounded_text(
                self.error_code,
                field="error_code",
                max_chars=64,
                required=False,
            )
            or None,
        )
        object.__setattr__(self, "error_message_sanitized", _safe_error(self.error_message_sanitized))
        limitations = tuple(
            dict.fromkeys(
                bounded_text(
                    item,
                    field=f"limitations[{index}]",
                    max_chars=500,
                    required=False,
                )
                for index, item in enumerate(self.limitations)
                if str(item or "").strip()
            )
        )[:32]
        object.__setattr__(self, "limitations", limitations)

    @property
    def items(self) -> tuple[Mapping[str, Any], ...]:
        return _normalized_items(
            self.normalized,
            as_of=self.as_of,
            available_at=self.available_at,
        )

    @property
    def citations(self) -> tuple[EvidenceCitation, ...]:
        if not self.items:
            return ()
        if self.content_hash is None:
            raise ValueError("news_search collection must be persisted before citation construction")
        citations: list[EvidenceCitation] = []
        for index, item in enumerate(self.items):
            pointer = f"/items/{index}/snippet"
            citations.append(
                EvidenceCitation(
                    id=_identifier(
                        "citation",
                        {"artifact_hash": self.content_hash, "json_pointer": pointer},
                    ),
                    relation="context",
                    artifact_type="dataset",
                    artifact_hash=self.content_hash,
                    json_pointer=pointer,
                    value_hash=json_value_hash(self.normalized, pointer),
                    available_at=self.available_at,
                    source_name=item["source"],
                    title=item["title"],
                    excerpt=item["snippet"],
                    canonical_url=item.get("canonical_url"),
                )
            )
        return tuple(citations)

    @property
    def claims(self) -> tuple[ResearchClaim, ...]:
        citations = self.citations
        claims: list[ResearchClaim] = []
        for item, citation in zip(self.items, citations):
            statement = bounded_text(
                f"{item['source']} reports: {item['title']}. {item['snippet']}",
                field="reported_event.statement",
                max_chars=2_000,
                required=True,
            )
            claims.append(
                ResearchClaim(
                    id=_identifier(
                        "claim",
                        {"citation_id": citation.id, "kind": "reported_event"},
                    ),
                    kind="reported_event",
                    statement=statement,
                    status="partial",
                    citation_ids=(citation.id,),
                    limitations=("snippet_only_unverified_external_report",),
                    available_at=self.available_at,
                )
            )
        return tuple(claims)

    def bind_content_hash(self, content_hash: str) -> "EvidenceCollectionResult":
        """Return an immutable result bound to its persisted dataset hash."""

        normalized_hash = require_sha256(content_hash, field="content_hash")
        if self.content_hash is not None and self.content_hash != normalized_hash:
            raise ValueError("news_search collection is already bound to a different hash")
        return replace(self, content_hash=normalized_hash)

    def to_evidence_artifact(self) -> Optional[EvidenceArtifact]:
        if self.content_hash is None:
            if self.items:
                raise ValueError("news_search collection must be persisted before evidence build")
            return None
        return EvidenceArtifact(
            artifact_type="dataset",
            artifact_hash=self.content_hash,
            stock_code=self.stock_code,
            available_at=self.available_at,
            payload=self.normalized,
            source_name="",
        )

    def to_dataset_input(self, market: str) -> "DatasetSnapshotInput":
        """Project the generic news_search persistence input."""

        from .repositories import DatasetSnapshotInput

        return DatasetSnapshotInput(
            dataset=NEWS_SEARCH_DATASET,
            scope_type="stock",
            scope_value=self.stock_code,
            market=bounded_text(market, field="market", max_chars=32, required=True),
            provider=self.provider,
            schema_version=NEWS_SEARCH_SCHEMA_VERSION,
            data_as_of=self.as_of,
            available_at=self.available_at,
            observed_at=self.observed_at,
            status=self.status,
            normalized=self.normalized,
            raw_ref=self.raw_ref,
            error_code=self.error_code,
            error_message_sanitized=self.error_message_sanitized,
            knowledge_as_of=self.as_of,
            retryable=self.status == DatasetStatus.FETCH_FAILED.value,
        )


def _empty_result(
    *,
    stock_code: str,
    as_of: datetime,
    status: str,
    provider: str,
    searched: bool,
    limitation: str,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
) -> EvidenceCollectionResult:
    normalized = {
        "schema_version": NEWS_SEARCH_SCHEMA_VERSION,
        "items": [],
        "limitations": [limitation],
    }
    return EvidenceCollectionResult(
        stock_code=stock_code,
        as_of=as_of,
        observed_at=as_of,
        status=status,
        provider=provider,
        normalized=normalized,
        available_at=as_of,
        error_code=error_code,
        error_message_sanitized=error_message,
        searched=searched,
        limitations=(limitation,),
    )


def _normalize_search_item(
    value: Any,
    *,
    as_of: datetime,
) -> Mapping[str, Any]:
    title = bounded_text(
        _field(value, "title"),
        field="search_result.title",
        max_chars=MAX_EVIDENCE_TITLE_CHARS,
        required=True,
    )
    snippet = bounded_text(
        _field(value, "snippet"),
        field="search_result.snippet",
        max_chars=MAX_EVIDENCE_EXCERPT_CHARS,
        required=True,
    )
    canonical_url = safe_canonical_url(_field(value, "url"), field="search_result.url")
    source = bounded_text(
        _field(value, "source"),
        field="search_result.source",
        max_chars=160,
        required=True,
    )
    published_at = _publication_time(
        _field(value, "published_date", _field(value, "published_at")),
        boundary=as_of,
    )
    normalized: dict[str, Any] = {
        "title": title,
        "snippet": snippet,
        "source": source,
        "available_at": as_of,
        "canonical_url": canonical_url,
    }
    normalized["published_at"] = published_at
    return MappingProxyType(canonicalize(normalized, exclude_volatile=False))


class EvidenceCollector:
    """Collect one bounded search page or replay an existing frozen result."""

    def __init__(
        self,
        search_callable: Optional[Callable[..., Any]] = None,
        repository: Optional["ResearchSnapshotRepository"] = None,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self.search_callable = search_callable
        self.repository = repository
        if not callable(clock):
            raise TypeError("clock must be callable")
        self.clock = clock

    def _live_observation_time(self, requested_as_of: datetime) -> datetime:
        observed_at = require_aware_utc(self.clock(), field="clock()")
        if observed_at < requested_as_of:
            raise ValueError("live evidence observation cannot precede requested as_of")
        return observed_at

    def collect(
        self,
        stock_code: str,
        *,
        stock_name: str = "",
        as_of: datetime,
        reference_mode: str = "historical",
        existing: Any = None,
        cancel_event: Any = None,
    ) -> EvidenceCollectionResult:
        boundary = require_aware_utc(as_of, field="as_of")
        normalized_stock = bounded_text(
            stock_code,
            field="stock_code",
            max_chars=64,
            required=True,
        )
        mode = str(reference_mode or "").strip().casefold()
        if mode not in {"live", "historical"}:
            raise ValueError("reference_mode must be 'live' or 'historical'")
        _raise_if_cancelled(cancel_event)
        if existing is not None:
            replay = hydrate_evidence_collection(existing)
            if replay.stock_code != normalized_stock:
                raise ValueError("existing evidence stock_code does not match request")
            if mode == "historical":
                if replay.observed_at > boundary or replay.available_at > boundary:
                    raise ValueError("historical evidence crosses the requested as_of boundary")
                if replay.as_of != boundary:
                    replay = replace(replay, as_of=boundary, searched=False)
            elif replay.as_of < boundary:
                raise ValueError("existing live evidence predates the requested research boundary")
            _raise_if_cancelled(cancel_event)
            return replay
        if mode == "historical":
            return _empty_result(
                stock_code=normalized_stock,
                as_of=boundary,
                status=DatasetStatus.PARTIAL.value,
                provider="historical_replay",
                searched=False,
                limitation="historical_evidence_unavailable",
                error_code="historical_evidence_unavailable",
            )
        if self.search_callable is None:
            observed_at = self._live_observation_time(boundary)
            return _empty_result(
                stock_code=normalized_stock,
                as_of=observed_at,
                status=DatasetStatus.FETCH_FAILED.value,
                provider="not_configured",
                searched=False,
                limitation="news_search_not_configured",
                error_code="search_not_configured",
            )
        try:
            response = self.search_callable(
                stock_code=normalized_stock,
                stock_name=bounded_text(
                    stock_name,
                    field="stock_name",
                    max_chars=160,
                    required=False,
                ),
                max_results=MAX_LIVE_SEARCH_RESULTS,
            )
        except Exception as exc:  # noqa: BLE001 - provider failure becomes explicit dataset state.
            _raise_if_cancelled(cancel_event)
            observed_at = self._live_observation_time(boundary)
            return _empty_result(
                stock_code=normalized_stock,
                as_of=observed_at,
                status=DatasetStatus.FETCH_FAILED.value,
                provider="search_provider",
                searched=True,
                limitation="news_search_fetch_failed",
                error_code="search_fetch_failed",
                error_message=str(exc),
            )
        _raise_if_cancelled(cancel_event)
        observed_at = self._live_observation_time(boundary)
        provider = bounded_text(
            _field(response, "provider", "search_provider"),
            field="search_response.provider",
            max_chars=128,
            required=True,
        )
        if not bool(_field(response, "success", False)):
            return _empty_result(
                stock_code=normalized_stock,
                as_of=observed_at,
                status=DatasetStatus.FETCH_FAILED.value,
                provider=provider,
                searched=True,
                limitation="news_search_fetch_failed",
                error_code="search_fetch_failed",
                error_message=_field(response, "error_message"),
            )
        raw_results = _field(response, "results", ())
        if not isinstance(raw_results, Sequence) or isinstance(raw_results, (str, bytes, bytearray)):
            return _empty_result(
                stock_code=normalized_stock,
                as_of=observed_at,
                status=DatasetStatus.FETCH_FAILED.value,
                provider=provider,
                searched=True,
                limitation="news_search_invalid_response",
                error_code="search_invalid_response",
            )
        items: list[Mapping[str, Any]] = []
        rejected = 0
        for raw_item in raw_results[:MAX_LIVE_SEARCH_RESULTS]:
            try:
                items.append(_normalize_search_item(raw_item, as_of=observed_at))
            except (TypeError, ValueError):
                rejected += 1
        if not items:
            status = DatasetStatus.EMPTY.value if not raw_results else DatasetStatus.PARTIAL.value
            limitation = "news_search_empty" if not raw_results else "news_search_results_rejected"
            return _empty_result(
                stock_code=normalized_stock,
                as_of=observed_at,
                status=status,
                provider=provider,
                searched=True,
                limitation=limitation,
                error_code=("search_results_rejected" if raw_results else None),
            )
        limitations = ("news_search_results_rejected",) if rejected else ()
        status = DatasetStatus.PARTIAL.value if rejected else DatasetStatus.AVAILABLE.value
        normalized = {
            "schema_version": NEWS_SEARCH_SCHEMA_VERSION,
            "items": [_plain(item) for item in items],
            "limitations": list(limitations),
        }
        return EvidenceCollectionResult(
            stock_code=normalized_stock,
            as_of=observed_at,
            observed_at=observed_at,
            status=status,
            provider=provider,
            normalized=normalized,
            available_at=observed_at,
            searched=True,
            limitations=limitations,
        )

    def persist(
        self,
        result: EvidenceCollectionResult,
        *,
        market: str,
        lease: "LeaseFence",
        now: Optional[datetime] = None,
    ) -> EvidenceCollectionResult:
        """Persist through the injected repository and bind the returned hash."""

        if self.repository is None:
            raise RuntimeError("EvidenceCollector.persist requires an injected repository")
        write_result = self.repository.write_dataset(
            result.to_dataset_input(market),
            lease=lease,
            now=now,
        )
        return result.bind_content_hash(write_result.content_hash)


def hydrate_evidence_collection(value: Any) -> EvidenceCollectionResult:
    """Hydrate a persisted news_search dataset or return an existing result."""

    if isinstance(value, EvidenceCollectionResult):
        return value
    if not isinstance(value, Mapping):
        raise TypeError("existing evidence collection must be a mapping")
    normalized = value.get("normalized", value.get("data", value.get("payload")))
    if isinstance(normalized, str):
        import json

        try:
            normalized = json.loads(normalized)
        except json.JSONDecodeError as exc:
            raise ValueError("persisted news_search normalized payload is invalid JSON") from exc
    if not isinstance(normalized, Mapping):
        raise TypeError("persisted news_search normalized payload must be a mapping")
    dataset_name = value.get("dataset")
    if dataset_name is not None and str(dataset_name) != NEWS_SEARCH_DATASET:
        raise ValueError("persisted dataset is not news_search evidence")
    as_of = value.get("knowledge_as_of", value.get("data_as_of", value.get("as_of")))
    available_at = value.get("available_at")
    observed_at = value.get("observed_at", available_at)
    stock_code = value.get("scope_value", value.get("stock_code"))
    return EvidenceCollectionResult(
        stock_code=stock_code,
        as_of=as_of,
        observed_at=observed_at,
        status=value.get("status"),
        provider=value.get("provider", "persisted_news_search"),
        normalized=normalized,
        available_at=available_at,
        content_hash=value.get("content_hash", value.get("hash")),
        raw_ref=value.get("raw_ref"),
        error_code=value.get("error_code"),
        error_message_sanitized=value.get(
            "error_message_sanitized",
            value.get("error_message"),
        ),
        searched=False,
        limitations=tuple(normalized.get("limitations") or ()),
    )


__all__ = [
    "EvidenceCollectionCancelledError",
    "EvidenceCollectionResult",
    "EvidenceCollector",
    "MAX_LIVE_SEARCH_RESULTS",
    "NEWS_SEARCH_DATASET",
    "NEWS_SEARCH_SCHEMA_VERSION",
    "hydrate_evidence_collection",
]
