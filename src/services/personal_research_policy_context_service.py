"""Server-side Portfolio Policy context for formal personal research.

The public personal-research API only accepts an account id and a proposed
target weight.  This service resolves every portfolio and sector fact from
server-owned, point-in-time records and emits a content-addressed audit
snapshot.  Missing facts remain explicit and force the deterministic gate to
fail closed; they are never replaced with zeroes.
"""

from __future__ import annotations

from datetime import date, datetime, time
import math
from typing import Any, Mapping, Optional

from data_provider.base import normalize_stock_code
from src.core.trading_calendar import get_market_for_stock
from src.repositories.portfolio_repo import PortfolioRepository
from src.services.portfolio_service import PortfolioService
from src.services.research.canonical import canonical_hash, canonicalize
from src.services.research.repositories import ResearchSnapshotRepository
from src.storage import DatabaseManager


POLICY_CONTEXT_CONTRACT_VERSION = "portfolio-policy-context-v1"


class PersonalResearchPolicyContextService:
    """Build one deterministic Policy Gate input without client-supplied facts."""

    def __init__(
        self,
        *,
        db_manager: Optional[DatabaseManager] = None,
        portfolio_service: Optional[PortfolioService] = None,
        research_repository: Optional[ResearchSnapshotRepository] = None,
    ) -> None:
        db = db_manager or DatabaseManager.get_instance()
        self.portfolios = portfolio_service or PortfolioService(
            repo=PortfolioRepository(db_manager=db)
        )
        self.research = research_repository or ResearchSnapshotRepository(db)

    def build(
        self,
        *,
        account_id: Optional[int],
        stock_code: str,
        target_weight_pct: Optional[float],
        entry_price: Any,
        stop_loss: Any,
        proposed_account_action: Optional[str] = None,
        as_of: Optional[date] = None,
        decision_session_date: Optional[date] = None,
    ) -> dict[str, Any]:
        valuation_bar_date = as_of or date.today()
        decision_date = decision_session_date or valuation_bar_date
        code = _stock_identity(stock_code)
        market = get_market_for_stock(code)
        reasons: set[str] = set()
        account_snapshot: Optional[Mapping[str, Any]] = None

        normalized_account_id = _positive_int_or_none(account_id)
        normalized_target_weight = _percentage_or_none(target_weight_pct)
        normalized_action = str(proposed_account_action or "").strip().lower() or None
        normalized_entry = _positive_number_or_none(entry_price)
        normalized_stop = _positive_number_or_none(stop_loss)
        if normalized_account_id is None:
            reasons.add("policy_account_missing")
        if normalized_target_weight is None:
            reasons.add("target_weight_missing")
        elif normalized_action in {"open_candidate", "add_candidate"} and (
            normalized_target_weight <= 0.0
        ):
            reasons.add("risk_increasing_target_weight_not_positive")
        if normalized_entry is None:
            reasons.add("entry_price_missing")
        if normalized_stop is None:
            reasons.add("stop_loss_missing")
        elif normalized_entry is not None and normalized_stop >= normalized_entry:
            reasons.add("stop_loss_not_below_entry")

        if normalized_account_id is not None:
            try:
                account_snapshot = self.portfolios.get_policy_valuation_state(
                    account_id=normalized_account_id,
                    as_of=valuation_bar_date,
                )
            except (LookupError, RuntimeError, ValueError):
                reasons.add("portfolio_snapshot_unavailable")

        positions: list[dict[str, Any]] = []
        target_sector: Optional[str] = None
        sector_dataset_hashes: set[str] = set()
        total_equity: Optional[float] = None
        if account_snapshot is not None:
            total_equity = _positive_number_or_none(
                account_snapshot.get("total_equity")
            )
            if total_equity is None:
                reasons.add("portfolio_equity_unavailable")
            if account_snapshot.get("fx_stale") is True:
                reasons.add("portfolio_fx_stale")
            for raw_position in account_snapshot.get("positions") or ():
                if not isinstance(raw_position, Mapping):
                    reasons.add("portfolio_position_invalid")
                    continue
                quantity = _positive_number_or_none(raw_position.get("quantity"))
                if quantity is None:
                    continue
                position_code = _stock_identity(raw_position.get("symbol"))
                position_market = str(raw_position.get("market") or "").strip().lower()
                market_value = _nonnegative_number_or_none(
                    raw_position.get("market_value_base")
                )
                if raw_position.get("price_available") is not True or market_value is None:
                    reasons.add("portfolio_position_price_unavailable")
                if raw_position.get("price_stale") is True:
                    reasons.add("portfolio_position_price_stale")
                sector, dataset_hash = self._sector_for_stock(
                    position_code,
                    as_of=decision_date,
                )
                if sector is None or dataset_hash is None:
                    reasons.add("portfolio_position_sector_unavailable")
                else:
                    sector_dataset_hashes.add(dataset_hash)
                positions.append(
                    {
                        "stock_code": position_code,
                        "market": position_market,
                        "quantity": quantity,
                        "market_value_base": market_value,
                        "price_date": raw_position.get("price_date"),
                        "sector": sector,
                        "sector_dataset_hash": dataset_hash,
                    }
                )

        target_sector, target_sector_hash = self._sector_for_stock(
            code,
            as_of=decision_date,
        )
        if target_sector is None or target_sector_hash is None:
            reasons.add("target_sector_unavailable")
        else:
            sector_dataset_hashes.add(target_sector_hash)

        positions.sort(key=lambda item: (item["market"], item["stock_code"]))
        audit_snapshot = canonicalize(
            {
                "contract_version": POLICY_CONTEXT_CONTRACT_VERSION,
                "account_id": normalized_account_id,
                "as_of": decision_date.isoformat(),
                "decision_session_date": decision_date.isoformat(),
                "valuation_bar_date": valuation_bar_date.isoformat(),
                "stock_code": code,
                "market": market,
                "target_weight_pct": normalized_target_weight,
                "proposed_account_action": normalized_action,
                "entry_price": normalized_entry,
                "stop_loss": normalized_stop,
                "total_equity_base": total_equity,
                "target_sector": target_sector,
                "sector_dataset_hashes": sorted(sector_dataset_hashes),
                "positions": positions,
                "source_data_quality": (
                    account_snapshot.get("data_quality")
                    if account_snapshot is not None
                    else None
                ),
                "source_limitations": sorted(
                    {
                        str(item)
                        for item in (
                            account_snapshot.get("limitations") or ()
                            if account_snapshot is not None
                            else ()
                        )
                        if str(item).strip()
                    }
                ),
            },
            exclude_volatile=False,
        )
        audit_hash = canonical_hash(audit_snapshot, exclude_volatile=False)

        current_weight: Optional[float] = None
        projected_weight: Optional[float] = None
        projected_sector_weight: Optional[float] = None
        position_risk: Optional[float] = None
        if not reasons and total_equity is not None:
            current_market_value = sum(
                float(item["market_value_base"] or 0.0)
                for item in positions
                if item["stock_code"] == code and item["market"] == market
            )
            current_weight = _rounded_percentage(
                current_market_value / total_equity * 100.0
            )
            projected_weight = _rounded_percentage(normalized_target_weight)
            existing_sector_value = sum(
                float(item["market_value_base"] or 0.0)
                for item in positions
                if item["sector"] == target_sector
                and (item["market"], item["stock_code"]) != (market, code)
            )
            projected_sector_value = (
                existing_sector_value + total_equity * normalized_target_weight / 100.0
            )
            projected_sector_weight = _rounded_percentage(
                projected_sector_value / total_equity * 100.0
            )
            position_risk = _rounded_percentage(
                normalized_target_weight
                * abs(normalized_entry - normalized_stop)
                / normalized_entry
            )

        return {
            "portfolio_complete": not reasons,
            "portfolio_snapshot_ref": (
                f"{POLICY_CONTEXT_CONTRACT_VERSION}:{audit_hash}"
            ),
            "current_position_weight_pct": current_weight,
            "projected_position_weight_pct": projected_weight,
            "projected_sector_weight_pct": projected_sector_weight,
            "position_risk_pct": position_risk,
            "incomplete_reason_codes": sorted(reasons),
            "audit_snapshot_hash": audit_hash,
            "audit_snapshot": audit_snapshot,
        }

    def _sector_for_stock(
        self,
        stock_code: str,
        *,
        as_of: date,
    ) -> tuple[Optional[str], Optional[str]]:
        candidates: dict[str, dict[str, Any]] = {}
        cutoff = datetime.combine(as_of, time.max)
        for scope in _scope_candidates(stock_code):
            for dataset in self.research.list_datasets(
                scope_value=scope,
                dataset="stock_basic",
                as_of=cutoff,
                limit=20,
            ):
                digest = str(dataset.get("content_hash") or "")
                if digest:
                    candidates[digest] = dataset
        ordered = sorted(
            candidates.values(),
            key=lambda item: (
                str(item.get("available_at") or ""),
                int(item.get("id") or 0),
            ),
            reverse=True,
        )
        for dataset in ordered:
            if dataset.get("status") not in {"available", "partial"}:
                continue
            rows = dataset.get("normalized")
            if isinstance(rows, Mapping):
                rows = [rows]
            if not isinstance(rows, list):
                continue
            sectors = {
                str(row.get("industry") or "").strip()
                for row in rows
                if isinstance(row, Mapping)
                and _stock_identity(row.get("ts_code") or row.get("symbol"))
                == stock_code
                and str(row.get("industry") or "").strip()
            }
            if len(sectors) == 1:
                return next(iter(sectors)), str(dataset["content_hash"])
            if len(sectors) > 1:
                return None, None
        return None, None


def _stock_identity(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw.startswith(("SH", "SZ", "BJ")) and raw[2:].isdigit():
        raw = raw[2:]
    if "." in raw:
        left, right = raw.rsplit(".", 1)
        if right in {"SH", "SZ", "BJ"}:
            raw = left
    normalized = normalize_stock_code(raw)
    return str(normalized or raw).strip().upper()


def _scope_candidates(stock_code: str) -> tuple[str, ...]:
    code = _stock_identity(stock_code)
    market = get_market_for_stock(code)
    values = [code]
    if code.isdigit() and len(code) == 6:
        suffix = "SH" if code.startswith(("5", "6", "9")) else "BJ" if code.startswith(("4", "8")) else "SZ"
        values.extend((f"{code}.{suffix}", f"{suffix}{code}"))
    if market:
        values.append(f"{market}:{code}")
    return tuple(dict.fromkeys(values))


def _positive_int_or_none(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _number_or_none(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _positive_number_or_none(value: Any) -> Optional[float]:
    parsed = _number_or_none(value)
    return parsed if parsed is not None and parsed > 0.0 else None


def _nonnegative_number_or_none(value: Any) -> Optional[float]:
    parsed = _number_or_none(value)
    return parsed if parsed is not None and parsed >= 0.0 else None


def _percentage_or_none(value: Any) -> Optional[float]:
    parsed = _number_or_none(value)
    return parsed if parsed is not None and 0.0 <= parsed <= 100.0 else None


def _rounded_percentage(value: Any) -> float:
    return round(float(value), 8)


__all__ = [
    "POLICY_CONTEXT_CONTRACT_VERSION",
    "PersonalResearchPolicyContextService",
]
