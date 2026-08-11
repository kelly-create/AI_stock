# -*- coding: utf-8 -*-
"""Two-phase Portfolio opening/reconciliation workflow."""

from __future__ import annotations

import hashlib
import json
import secrets
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

from src.repositories.portfolio_reconciliation_repo import PortfolioReconciliationRepository
from src.repositories.portfolio_repo import PortfolioRepository
from src.services.portfolio_service import EPS, PortfolioService
from src.storage import utc_naive_now


RECONCILIATION_CONTRACT_VERSION = "portfolio-reconciliation-v1"
PREVIEW_TTL = timedelta(minutes=15)


class PortfolioReconciliationConflictError(Exception):
    """Typed 409 error for stale, expired, or conflicting reconciliation state."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


class PortfolioReconciliationService:
    """Preview authoritative broker state and atomically append state-set events."""

    def __init__(
        self,
        *,
        portfolio_service: Optional[PortfolioService] = None,
        portfolio_repo: Optional[PortfolioRepository] = None,
        reconciliation_repo: Optional[PortfolioReconciliationRepository] = None,
    ) -> None:
        if portfolio_service is not None:
            self.portfolio = portfolio_service
            self.portfolio_repo = portfolio_service.repo
            self.repo = reconciliation_repo or portfolio_service.reconciliation_repo
            return
        self.portfolio_repo = portfolio_repo or PortfolioRepository()
        self.repo = reconciliation_repo or PortfolioReconciliationRepository(
            db_manager=self.portfolio_repo.db
        )
        self.portfolio = PortfolioService(
            repo=self.portfolio_repo,
            reconciliation_repo=self.repo,
        )

    def preview(
        self,
        *,
        account_id: int,
        event_type: str,
        effective_date: date,
        cash: Iterable[Dict[str, Any]],
        positions: Iterable[Dict[str, Any]],
        source: str,
        note: Optional[str],
    ) -> Dict[str, Any]:
        event_type_norm = self._normalize_event_type(event_type)
        if effective_date > date.today():
            raise ValueError("effective_date cannot be in the future")
        source_norm = (source or "").strip()
        if not source_norm or len(source_norm) > 64:
            raise ValueError("source must be 1-64 characters")
        note_norm = (note or "").strip() or None
        if note_norm is not None and len(note_norm) > 255:
            raise ValueError("note must be at most 255 characters")
        target = self._normalize_target(cash=cash, positions=positions)
        token = secrets.token_urlsafe(32)
        token_hash = self._sha256_text(token)
        expires_at = utc_naive_now() + PREVIEW_TTL

        with self.portfolio_repo.portfolio_write_session() as session:
            account = self.portfolio_repo.get_account_in_session(
                session=session,
                account_id=account_id,
            )
            if account is None:
                raise ValueError(f"Account not found or inactive: {account_id}")
            self._validate_opening_boundary(
                session=session,
                account_id=account_id,
                event_type=event_type_norm,
                effective_date=effective_date,
            )
            before = self.portfolio.get_reconciliation_book_state(
                account_id=account_id,
                as_of=effective_date,
                event_type=event_type_norm,
                session=session,
            )
            book_hash = self._hash_json(before)
            target_hash = self._hash_json(target)
            adjustments, warnings = self._build_adjustments(before=before, target=target)
            request_payload = {
                "contract_version": RECONCILIATION_CONTRACT_VERSION,
                "event_type": event_type_norm,
                "effective_date": effective_date.isoformat(),
                "source": source_norm,
                "note": note_norm,
                "book_hash": book_hash,
                "target_hash": target_hash,
                "target": target,
            }
            input_hash = self._hash_json(
                {
                    key: request_payload[key]
                    for key in (
                        "contract_version",
                        "event_type",
                        "effective_date",
                        "source",
                        "note",
                        "target_hash",
                        "target",
                    )
                }
            )
            diff_payload = {
                "contract_version": RECONCILIATION_CONTRACT_VERSION,
                "book_hash": book_hash,
                "target_hash": target_hash,
                "adjustments": adjustments,
                "warnings": warnings,
            }
            row = self.repo.create_preview_in_session(
                session=session,
                account_id=account_id,
                event_type=event_type_norm,
                effective_date=effective_date,
                preview_token_hash=token_hash,
                input_hash=input_hash,
                request_json=self._canonical_json(request_payload),
                diff_json=self._canonical_json(diff_payload),
                note=note_norm,
                expires_at=expires_at,
            )
            preview_id = int(row.id)

        return {
            "id": preview_id,
            "preview_token": token,
            "event_type": event_type_norm,
            "effective_date": effective_date.isoformat(),
            "expires_at": expires_at.isoformat(),
            "input_hash": input_hash,
            "book_hash": book_hash,
            "target_hash": target_hash,
            "diff": {"adjustments": adjustments},
            "warnings": warnings,
        }

    def apply(
        self,
        *,
        account_id: int,
        preview_token: str,
        idempotency_key: str,
    ) -> Dict[str, Any]:
        token = (preview_token or "").strip()
        if len(token) < 20 or len(token) > 256:
            raise ValueError("preview_token is invalid")
        idempotency = (idempotency_key or "").strip()
        if not idempotency or len(idempotency) > 128:
            raise ValueError("idempotency_key must be 1-128 characters")
        token_hash = self._sha256_text(token)
        apply_started_at = utc_naive_now()
        expired = False
        result: Optional[Dict[str, Any]] = None
        with self.portfolio_repo.portfolio_write_session() as session:
            account = self.portfolio_repo.get_account_in_session(
                session=session,
                account_id=account_id,
            )
            if account is None:
                raise ValueError(f"Account not found or inactive: {account_id}")
            row = self.repo.get_by_token_hash_in_session(
                session=session,
                account_id=account_id,
                preview_token_hash=token_hash,
            )
            if row is None:
                raise PortfolioReconciliationConflictError(
                    "preview_not_found",
                    "Reconciliation preview was not found",
                )
            if row.status == "expired" or (
                row.status == "preview" and row.expires_at <= apply_started_at
            ):
                self.repo.mark_expired_in_session(session=session, row=row)
                expired = True
            else:
                result = self._apply_available_preview_in_session(
                    session=session,
                    row=row,
                    account_id=account_id,
                    idempotency_key=idempotency,
                )

        if expired:
            raise PortfolioReconciliationConflictError(
                "preview_expired",
                "Reconciliation preview expired; create a new preview",
            )
        if result is None:  # pragma: no cover - defensive state-machine guard
            raise RuntimeError("Reconciliation apply produced no result")
        return result

    def _apply_available_preview_in_session(
        self,
        *,
        session: Any,
        row: Any,
        account_id: int,
        idempotency_key: str,
    ) -> Dict[str, Any]:
        existing_idempotent = self.repo.get_by_idempotency_key_in_session(
            session=session,
            account_id=account_id,
            idempotency_key=idempotency_key,
        )
        if row.status == "applied":
            if row.idempotency_key == idempotency_key:
                return self._header_to_public(row)
            raise PortfolioReconciliationConflictError(
                "preview_already_applied",
                "Reconciliation preview was already applied with another idempotency key",
            )
        if existing_idempotent is not None and int(existing_idempotent.id) != int(row.id):
            raise PortfolioReconciliationConflictError(
                "idempotency_conflict",
                "idempotency_key is already bound to another reconciliation",
            )
        if row.status != "preview":
            raise PortfolioReconciliationConflictError(
                "preview_unavailable",
                f"Reconciliation preview is {row.status}",
            )

        try:
            request_payload = self._load_object(row.request_json, field="request_json")
            diff_payload = self._load_object(row.diff_json, field="diff_json")
        except ValueError as exc:
            raise PortfolioReconciliationConflictError(
                "preview_integrity_error",
                "Persisted reconciliation preview JSON is invalid",
            ) from exc
        self._validate_persisted_contract(
            row=row,
            request_payload=request_payload,
            diff_payload=diff_payload,
        )
        self._validate_opening_boundary(
            session=session,
            account_id=account_id,
            event_type=row.event_type,
            effective_date=row.effective_date,
        )
        current = self.portfolio.get_reconciliation_book_state(
            account_id=account_id,
            as_of=row.effective_date,
            event_type=row.event_type,
            session=session,
        )
        current_hash = self._hash_json(current)
        if current_hash != request_payload["book_hash"]:
            raise PortfolioReconciliationConflictError(
                "stale_preview",
                "Portfolio ledger changed after preview; create a new preview",
            )

        target = request_payload["target"]
        adjustments, warnings = self._build_adjustments(before=current, target=target)
        expected_diff = {
            "contract_version": RECONCILIATION_CONTRACT_VERSION,
            "book_hash": current_hash,
            "target_hash": self._hash_json(target),
            "adjustments": adjustments,
            "warnings": warnings,
        }
        if self._canonical_json(expected_diff) != self._canonical_json(diff_payload):
            raise PortfolioReconciliationConflictError(
                "preview_integrity_error",
                "Persisted reconciliation diff no longer matches its inputs",
            )
        event_version = self.repo.next_event_version_in_session(
            session=session,
            account_id=account_id,
        )
        adjustment_rows = [self._adjustment_to_row(item) for item in adjustments]
        self.repo.apply_preview_in_session(
            session=session,
            row=row,
            event_version=event_version,
            idempotency_key=idempotency_key,
            adjustments=adjustment_rows,
        )
        result = self._header_to_public(row)
        result["adjustment_count"] = len(adjustments)
        return result

    def list_records(self, *, account_id: int, include_previews: bool = False) -> List[Dict[str, Any]]:
        self._require_account(account_id)
        if include_previews:
            self._expire_due_previews(account_id=account_id)
        return [
            self._header_to_public(row)
            for row in self.repo.list_records(
                account_id=account_id,
                include_previews=include_previews,
            )
        ]

    def get_record(self, *, account_id: int, reconciliation_id: int) -> Optional[Dict[str, Any]]:
        self._require_account(account_id)
        self._expire_due_previews(account_id=account_id)
        found = self.repo.get_record(
            account_id=account_id,
            reconciliation_id=reconciliation_id,
        )
        if found is None:
            return None
        header, adjustments = found
        result = self._header_to_public(header)
        result["adjustments"] = [self._adjustment_to_public(row) for row in adjustments]
        request = self._load_object(header.request_json, field="request_json")
        result["target"] = request.get("target")
        return result

    def _expire_due_previews(self, *, account_id: int) -> None:
        with self.portfolio_repo.portfolio_write_session() as session:
            self.repo.expire_due_previews_in_session(
                session=session,
                account_id=account_id,
                now=utc_naive_now(),
            )

    def _validate_opening_boundary(
        self,
        *,
        session: Any,
        account_id: int,
        event_type: str,
        effective_date: date,
    ) -> None:
        if event_type != "opening":
            return
        if self.repo.has_any_applied_in_session(session=session, account_id=account_id):
            raise PortfolioReconciliationConflictError(
                "opening_already_exists",
                "An applied opening or reconciliation already exists for this account",
            )
        first_activity = self.portfolio_repo.get_first_activity_date_in_session(
            session=session,
            account_id=account_id,
            as_of=date.max,
        )
        if first_activity is not None and first_activity < effective_date:
            raise PortfolioReconciliationConflictError(
                "opening_after_ledger_activity",
                "Opening effective_date must be on or before the first ledger activity date",
            )

    def _normalize_target(
        self,
        *,
        cash: Iterable[Dict[str, Any]],
        positions: Iterable[Dict[str, Any]],
    ) -> Dict[str, Any]:
        cash_by_currency: Dict[str, Dict[str, Any]] = {}
        for raw in cash:
            if not isinstance(raw, dict):
                raise ValueError("cash entries must be objects")
            currency = self.portfolio._normalize_currency(raw.get("currency"))
            if currency in cash_by_currency:
                raise ValueError(f"Duplicate cash currency: {currency}")
            balance = self.portfolio._finite_float(raw.get("balance"), field="cash balance")
            if abs(balance) > EPS:
                cash_by_currency[currency] = {"currency": currency, "balance": balance}

        position_by_identity: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        for raw in positions:
            if not isinstance(raw, dict):
                raise ValueError("positions entries must be objects")
            stock_code = self.portfolio._normalize_symbol_for_position(raw.get("stock_code"))
            if not stock_code:
                raise ValueError("position stock_code is required")
            market = self.portfolio._normalize_market(raw.get("market"))
            currency = self.portfolio._normalize_currency(raw.get("currency"))
            quantity = self.portfolio._finite_float(raw.get("quantity"), field="position quantity")
            total_cost = self.portfolio._finite_float(raw.get("total_cost"), field="position total_cost")
            if quantity < -EPS or total_cost < -EPS:
                raise ValueError("position quantity and total_cost must be >= 0")
            if quantity <= EPS:
                if total_cost > EPS:
                    raise ValueError("A zero position quantity must have zero total_cost")
                continue
            identity = (market, stock_code, currency)
            if identity in position_by_identity:
                raise ValueError(f"Duplicate position identity: {market}/{stock_code}/{currency}")
            position_by_identity[identity] = {
                "stock_code": stock_code,
                "market": market,
                "currency": currency,
                "quantity": quantity,
                "total_cost": total_cost,
            }

        return {
            "cash": [cash_by_currency[key] for key in sorted(cash_by_currency)],
            "positions": [position_by_identity[key] for key in sorted(position_by_identity)],
        }

    def _build_adjustments(
        self,
        *,
        before: Dict[str, Any],
        target: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        adjustments: List[Dict[str, Any]] = []
        warnings: List[str] = []
        before_cash = {item["currency"]: item for item in before.get("cash", [])}
        target_cash = {item["currency"]: item for item in target.get("cash", [])}
        for currency in sorted(set(before_cash) | set(target_cash)):
            old = before_cash.get(currency, {"currency": currency, "balance": 0.0})
            new = target_cash.get(currency, {"currency": currency, "balance": 0.0})
            delta = float(new["balance"]) - float(old["balance"])
            if abs(delta) <= EPS:
                continue
            adjustments.append(
                {
                    "identity_key": f"cash:{currency}",
                    "adjustment_type": "cash",
                    "currency": currency,
                    "before": old,
                    "after": new,
                    "cash_delta": delta,
                    "quantity_delta": 0.0,
                    "total_cost_delta": 0.0,
                }
            )

        def position_key(item: Dict[str, Any]) -> Tuple[str, str, str]:
            return item["market"], item["stock_code"], item["currency"]

        before_positions = {position_key(item): item for item in before.get("positions", [])}
        target_positions = {position_key(item): item for item in target.get("positions", [])}
        for identity in sorted(set(before_positions) | set(target_positions)):
            market, stock_code, currency = identity
            zero = {
                "stock_code": stock_code,
                "market": market,
                "currency": currency,
                "quantity": 0.0,
                "total_cost": 0.0,
            }
            old = before_positions.get(identity, zero)
            new = target_positions.get(identity, zero)
            quantity_delta = float(new["quantity"]) - float(old["quantity"])
            total_cost_delta = float(new["total_cost"]) - float(old["total_cost"])
            # An absolute state-set seals aggregate quantity/cost and therefore
            # resets any prior FIFO lot lineage even when the aggregate values
            # happen to be unchanged at preview time.
            warnings.append(f"fifo_lineage_reset:{market}:{stock_code}:{currency}")
            if abs(quantity_delta) <= EPS and abs(total_cost_delta) <= EPS:
                continue
            adjustments.append(
                {
                    "identity_key": f"position:{market}:{stock_code}:{currency}",
                    "adjustment_type": "position",
                    "stock_code": stock_code,
                    "market": market,
                    "currency": currency,
                    "before": old,
                    "after": new,
                    "quantity_delta": quantity_delta,
                    "total_cost_delta": total_cost_delta,
                    "cash_delta": 0.0,
                }
            )
        return adjustments, warnings

    def _validate_persisted_contract(
        self,
        *,
        row: Any,
        request_payload: Dict[str, Any],
        diff_payload: Dict[str, Any],
    ) -> None:
        required_request_keys = {
            "book_hash",
            "contract_version",
            "effective_date",
            "event_type",
            "note",
            "source",
            "target",
            "target_hash",
        }
        if set(request_payload) != required_request_keys:
            raise PortfolioReconciliationConflictError(
                "preview_integrity_error",
                "Persisted reconciliation request has an invalid shape",
            )
        if request_payload.get("contract_version") != RECONCILIATION_CONTRACT_VERSION:
            raise PortfolioReconciliationConflictError(
                "preview_integrity_error",
                "Unsupported reconciliation preview contract",
            )
        if request_payload.get("event_type") != row.event_type:
            raise PortfolioReconciliationConflictError("preview_integrity_error", "Event type mismatch")
        if request_payload.get("effective_date") != row.effective_date.isoformat():
            raise PortfolioReconciliationConflictError("preview_integrity_error", "Effective date mismatch")
        if request_payload.get("note") != row.note:
            raise PortfolioReconciliationConflictError("preview_integrity_error", "Note mismatch")
        target = request_payload.get("target")
        if not isinstance(target, dict) or set(target) != {"cash", "positions"}:
            raise PortfolioReconciliationConflictError("preview_integrity_error", "Target shape mismatch")
        if not isinstance(target["cash"], list) or not isinstance(target["positions"], list):
            raise PortfolioReconciliationConflictError("preview_integrity_error", "Target shape mismatch")
        try:
            normalized_target = self._normalize_target(
                cash=target["cash"],
                positions=target["positions"],
            )
        except ValueError as exc:
            raise PortfolioReconciliationConflictError(
                "preview_integrity_error",
                "Persisted target is invalid",
            ) from exc
        if self._canonical_json(normalized_target) != self._canonical_json(target):
            raise PortfolioReconciliationConflictError("preview_integrity_error", "Target is not canonical")
        if self._hash_json(target) != request_payload.get("target_hash"):
            raise PortfolioReconciliationConflictError("preview_integrity_error", "Target hash mismatch")
        expected_input_hash = self._hash_json(
            {
                key: request_payload[key]
                for key in (
                    "contract_version",
                    "event_type",
                    "effective_date",
                    "source",
                    "note",
                    "target_hash",
                    "target",
                )
            }
        )
        if expected_input_hash != row.input_hash:
            raise PortfolioReconciliationConflictError("preview_integrity_error", "Input hash mismatch")
        if set(diff_payload) != {
            "adjustments",
            "book_hash",
            "contract_version",
            "target_hash",
            "warnings",
        }:
            raise PortfolioReconciliationConflictError(
                "preview_integrity_error",
                "Persisted reconciliation diff has an invalid shape",
            )
        if not isinstance(diff_payload["adjustments"], list) or not isinstance(
            diff_payload["warnings"], list
        ):
            raise PortfolioReconciliationConflictError(
                "preview_integrity_error",
                "Persisted reconciliation diff has invalid collections",
            )

    @staticmethod
    def _adjustment_to_row(item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "identity_key": item["identity_key"],
            "adjustment_type": item["adjustment_type"],
            "stock_code": item.get("stock_code"),
            "market": item.get("market"),
            "currency": item["currency"],
            "quantity_delta": item["quantity_delta"],
            "total_cost_delta": item["total_cost_delta"],
            "cash_delta": item["cash_delta"],
            "before_json": PortfolioReconciliationService._canonical_json(item["before"]),
            "after_json": PortfolioReconciliationService._canonical_json(item["after"]),
        }

    @staticmethod
    def _adjustment_to_public(row: Any) -> Dict[str, Any]:
        return {
            "id": int(row.id),
            "identity_key": row.identity_key,
            "adjustment_type": row.adjustment_type,
            "stock_code": row.stock_code,
            "market": row.market,
            "currency": row.currency,
            "quantity_delta": float(row.quantity_delta or 0.0),
            "total_cost_delta": float(row.total_cost_delta or 0.0),
            "cash_delta": float(row.cash_delta or 0.0),
            "before": PortfolioReconciliationService._load_object(row.before_json, field="before_json"),
            "after": PortfolioReconciliationService._load_object(row.after_json, field="after_json"),
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }

    @staticmethod
    def _header_to_public(row: Any) -> Dict[str, Any]:
        request = PortfolioReconciliationService._load_object(row.request_json, field="request_json")
        diff = PortfolioReconciliationService._load_object(row.diff_json, field="diff_json")
        source = request.get("source")
        warnings = diff.get("warnings")
        if not isinstance(source, str) or not source:
            raise ValueError("Invalid reconciliation source")
        if not isinstance(warnings, list) or any(not isinstance(item, str) for item in warnings):
            raise ValueError("Invalid reconciliation warnings")
        return {
            "id": int(row.id),
            "account_id": int(row.account_id),
            "event_type": row.event_type,
            "status": row.status,
            "event_version": int(row.event_version) if row.event_version is not None else None,
            "effective_date": row.effective_date.isoformat(),
            "input_hash": row.input_hash,
            "book_hash": diff.get("book_hash"),
            "target_hash": diff.get("target_hash"),
            "source": source,
            "note": row.note,
            "warnings": list(warnings),
            "expires_at": row.expires_at.isoformat() if row.expires_at else None,
            "applied_at": row.applied_at.isoformat() if row.applied_at else None,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }

    def _require_account(self, account_id: int) -> Any:
        account = self.portfolio_repo.get_account(account_id)
        if account is None:
            raise ValueError(f"Account not found or inactive: {account_id}")
        return account

    @staticmethod
    def _normalize_event_type(value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in {"opening", "adjustment"}:
            raise ValueError("event_type must be opening or adjustment")
        return normalized

    @staticmethod
    def _load_object(value: Any, *, field: str) -> Dict[str, Any]:
        try:
            payload = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid {field}") from exc
        if not isinstance(payload, dict):
            raise ValueError(f"Invalid {field}")
        return payload

    @staticmethod
    def _canonical_json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    @classmethod
    def _hash_json(cls, value: Any) -> str:
        return hashlib.sha256(cls._canonical_json(value).encode("utf-8")).hexdigest()

    @staticmethod
    def _sha256_text(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()
