# -*- coding: utf-8 -*-
"""Decision signal repository for Issue #1390 P1."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import and_, desc, func, or_, select
from sqlalchemy.exc import IntegrityError

from src.repositories.portfolio_policy_evaluation_repo import (
    PortfolioPolicyEvaluationRepository,
)
from src.schemas.decision_profile import (
    DECISION_PROFILE_FILTER_ALL,
    DecisionProfileFilter,
)
from src.storage import (
    AnalysisJobRecord,
    DatabaseManager,
    DecisionSignalRecord,
    to_utc_naive_datetime,
    utc_naive_now,
)


@dataclass
class DecisionSignalCreateResult:
    """Outcome of an idempotent DecisionSignal create attempt."""

    row: DecisionSignalRecord
    created: bool
    refreshed: bool = False
    duplicate: bool = False
    invalidation_reference_at: Optional[datetime] = None

    def __iter__(self):
        yield self.row
        yield self.created


class DecisionSignalRepository:
    """DB access layer for persisted AI decision signals."""

    _RELAXED_MERGE_STATUSES = frozenset({"active", "expired"})
    _IMMUTABLE_REFRESH_FIELDS = frozenset({
        "id",
        "created_at",
        "source_report_id",
        "source_type",
        "source_agent",
        "trace_id",
        "trigger_source",
        "market",
        "stock_code",
        "decision_profile",
        "action",
        "horizon",
        "market_phase",
        "research_stance",
        "account_action",
        "value_quality_score",
        "trend_timing_score",
        "catalyst_score",
        "risk_score",
        "evidence_quality_score",
        "research_snapshot_hash",
        "policy_version",
        "policy_hash",
        "policy_evaluation_hash",
        "portfolio_snapshot_ref",
        "prompt_version",
        "catalysts_json",
        "invalidators_json",
        "unknowns_json",
        "evidence_refs_json",
        "policy_mode",
        "policy_decision",
        "would_block",
        "policy_reasons_json",
    })

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager.get_instance()

    def create(self, fields: Dict[str, Any]) -> DecisionSignalRecord:
        fields = self._normalize_datetime_fields(fields)
        with self.db.get_session() as session:
            self._assert_current_durable_lease_in_session(session)
            row = DecisionSignalRecord(**fields)
            session.add(row)
            session.commit()
            session.refresh(row)
            return row

    def create_if_absent(
        self,
        fields: Dict[str, Any],
        *,
        allow_relaxed_horizon_fill: bool = False,
        policy_evaluation_fields: Optional[Dict[str, Any]] = None,
    ) -> DecisionSignalCreateResult:
        self.expire_due_signals()
        fields = self._normalize_datetime_fields(fields)
        with self.db.get_session() as session:
            # The heartbeat/checkpoint in DecisionSignalService is only a safe
            # boundary hint.  Revalidate the fencing token inside the same
            # SQLite write transaction that can refresh or insert a signal so
            # an expired attempt cannot race a reclaimed Worker.
            self._assert_current_durable_lease_in_session(session)
            existing = self._find_existing_in_session(session=session, fields=fields)
            if existing is not None:
                if self._should_refresh_existing(existing, fields):
                    self._refresh_existing_in_session(existing, fields)
                    PortfolioPolicyEvaluationRepository.ensure_in_session(
                        session=session,
                        signal_id=int(existing.id),
                        fields=policy_evaluation_fields,
                    )
                    session.commit()
                    session.refresh(existing)
                    return DecisionSignalCreateResult(
                        row=existing,
                        created=False,
                        refreshed=True,
                        invalidation_reference_at=existing.updated_at,
                    )
                _evaluation, evaluation_created = (
                    PortfolioPolicyEvaluationRepository.ensure_in_session(
                        session=session,
                        signal_id=int(existing.id),
                        fields=policy_evaluation_fields,
                    )
                )
                if evaluation_created:
                    session.commit()
                return DecisionSignalCreateResult(
                    row=existing,
                    created=False,
                    duplicate=True,
                    invalidation_reference_at=existing.created_at,
                )

            relaxed_existing = self._find_relaxed_existing_in_session(
                session=session,
                fields=fields,
                allow_relaxed_horizon_fill=allow_relaxed_horizon_fill,
            )
            if relaxed_existing is not None:
                if self._should_refresh_existing(relaxed_existing, fields):
                    self._refresh_existing_in_session(relaxed_existing, fields)
                    self._fill_relaxed_dimensions_in_session(
                        relaxed_existing,
                        fields,
                        allow_horizon_fill=allow_relaxed_horizon_fill,
                    )
                    PortfolioPolicyEvaluationRepository.ensure_in_session(
                        session=session,
                        signal_id=int(relaxed_existing.id),
                        fields=policy_evaluation_fields,
                    )
                    session.commit()
                    session.refresh(relaxed_existing)
                    return DecisionSignalCreateResult(
                        row=relaxed_existing,
                        created=False,
                        refreshed=True,
                        invalidation_reference_at=relaxed_existing.updated_at,
                    )
                if relaxed_existing.status == "active":
                    changed = self._fill_relaxed_dimensions_in_session(
                        relaxed_existing,
                        fields,
                        allow_horizon_fill=allow_relaxed_horizon_fill,
                    )
                    if changed:
                        PortfolioPolicyEvaluationRepository.ensure_in_session(
                            session=session,
                            signal_id=int(relaxed_existing.id),
                            fields=policy_evaluation_fields,
                        )
                        session.commit()
                        session.refresh(relaxed_existing)
                        return DecisionSignalCreateResult(
                            row=relaxed_existing,
                            created=False,
                            refreshed=True,
                            invalidation_reference_at=relaxed_existing.created_at,
                        )
                _evaluation, evaluation_created = (
                    PortfolioPolicyEvaluationRepository.ensure_in_session(
                        session=session,
                        signal_id=int(relaxed_existing.id),
                        fields=policy_evaluation_fields,
                    )
                )
                if evaluation_created:
                    session.commit()
                return DecisionSignalCreateResult(
                    row=relaxed_existing,
                    created=False,
                    duplicate=True,
                    invalidation_reference_at=relaxed_existing.created_at,
                )

            row = DecisionSignalRecord(**fields)
            session.add(row)
            try:
                session.flush()
                PortfolioPolicyEvaluationRepository.ensure_in_session(
                    session=session,
                    signal_id=int(row.id),
                    fields=policy_evaluation_fields,
                )
                session.commit()
            except IntegrityError:
                session.rollback()
                idempotency_key = fields.get("idempotency_key")
                if not idempotency_key:
                    raise
                existing = session.execute(
                    select(DecisionSignalRecord)
                    .where(DecisionSignalRecord.idempotency_key == idempotency_key)
                    .limit(1)
                ).scalar_one_or_none()
                if existing is None:
                    raise
                _evaluation, evaluation_created = (
                    PortfolioPolicyEvaluationRepository.ensure_in_session(
                        session=session,
                        signal_id=int(existing.id),
                        fields=policy_evaluation_fields,
                    )
                )
                if evaluation_created:
                    session.commit()
                return DecisionSignalCreateResult(
                    row=existing,
                    created=False,
                    duplicate=True,
                    invalidation_reference_at=existing.created_at,
                )
            session.refresh(row)
            return DecisionSignalCreateResult(
                row=row,
                created=True,
                invalidation_reference_at=row.created_at,
            )

    def get(self, signal_id: int) -> Optional[DecisionSignalRecord]:
        self.expire_due_signals()
        with self.db.get_session() as session:
            return session.execute(
                select(DecisionSignalRecord).where(DecisionSignalRecord.id == signal_id).limit(1)
            ).scalar_one_or_none()

    def list(
        self,
        *,
        stock_codes: Optional[List[str]] = None,
        stock_identities: Optional[List[Tuple[str, str]]] = None,
        market: Optional[str] = None,
        action: Optional[str] = None,
        market_phase: Optional[str] = None,
        decision_profile_filter: DecisionProfileFilter = DECISION_PROFILE_FILTER_ALL,
        source_type: Optional[str] = None,
        source_report_id: Optional[int] = None,
        trace_id: Optional[str] = None,
        trigger_source: Optional[str] = None,
        status: Optional[str] = None,
        created_from: Optional[datetime] = None,
        created_to: Optional[datetime] = None,
        expires_from: Optional[datetime] = None,
        expires_to: Optional[datetime] = None,
        page: int = 1,
        page_size: int = 20,
    ) -> Tuple[List[DecisionSignalRecord], int]:
        self.expire_due_signals()
        created_from = self._normalize_optional_datetime(created_from)
        created_to = self._normalize_optional_datetime(created_to)
        expires_from = self._normalize_optional_datetime(expires_from)
        expires_to = self._normalize_optional_datetime(expires_to)
        conditions = self._build_conditions(
            stock_codes=stock_codes,
            stock_identities=stock_identities,
            market=market,
            action=action,
            market_phase=market_phase,
            decision_profile_filter=decision_profile_filter,
            source_type=source_type,
            source_report_id=source_report_id,
            trace_id=trace_id,
            trigger_source=trigger_source,
            status=status,
            created_from=created_from,
            created_to=created_to,
            expires_from=expires_from,
            expires_to=expires_to,
        )
        where_clause = and_(*conditions) if conditions else True
        safe_page = max(1, int(page))
        safe_page_size = max(1, min(int(page_size), 100))
        offset = (safe_page - 1) * safe_page_size

        with self.db.get_session() as session:
            total = session.execute(
                select(func.count(DecisionSignalRecord.id))
                .select_from(DecisionSignalRecord)
                .where(where_clause)
            ).scalar() or 0
            rows = session.execute(
                select(DecisionSignalRecord)
                .where(where_clause)
                .order_by(desc(DecisionSignalRecord.created_at), desc(DecisionSignalRecord.id))
                .offset(offset)
                .limit(safe_page_size)
            ).scalars().all()
            return list(rows), int(total)

    def get_latest_active(
        self,
        *,
        stock_codes: List[str],
        market: Optional[str] = None,
        limit: int = 1,
    ) -> List[DecisionSignalRecord]:
        self.expire_due_signals()
        safe_limit = max(1, min(int(limit), 100))
        conditions = [
            DecisionSignalRecord.status == "active",
            DecisionSignalRecord.stock_code.in_(stock_codes),
        ]
        if market:
            conditions.append(DecisionSignalRecord.market == market)
        with self.db.get_session() as session:
            rows = session.execute(
                select(DecisionSignalRecord)
                .where(and_(*conditions))
                .order_by(desc(DecisionSignalRecord.created_at), desc(DecisionSignalRecord.id))
                .limit(safe_limit)
            ).scalars().all()
            return list(rows)

    def list_active_by_stock_actions(
        self,
        *,
        market: str,
        stock_code: str,
        actions: List[str],
        decision_profile: Optional[str],
        exclude_signal_id: Optional[int] = None,
    ) -> List[DecisionSignalRecord]:
        self.expire_due_signals()
        if not actions:
            return []
        conditions = [
            DecisionSignalRecord.status == "active",
            DecisionSignalRecord.market == market,
            DecisionSignalRecord.stock_code == stock_code,
            self._same_profile_condition(decision_profile),
            DecisionSignalRecord.action.in_(actions),
        ]
        if exclude_signal_id is not None:
            conditions.append(DecisionSignalRecord.id != exclude_signal_id)
        with self.db.get_session() as session:
            rows = session.execute(
                select(DecisionSignalRecord)
                .where(and_(*conditions))
                .order_by(desc(DecisionSignalRecord.created_at), desc(DecisionSignalRecord.id))
            ).scalars().all()
            return list(rows)

    def update_status(
        self,
        signal_id: int,
        *,
        status: str,
        metadata_json: Optional[str] = None,
        replace_metadata: bool = False,
    ) -> Optional[DecisionSignalRecord]:
        with self.db.get_session() as session:
            self._assert_current_durable_lease_in_session(session)
            row = session.execute(
                select(DecisionSignalRecord).where(DecisionSignalRecord.id == signal_id).limit(1)
            ).scalar_one_or_none()
            if row is None:
                return None
            row.status = status
            if replace_metadata:
                row.metadata_json = metadata_json
            row.updated_at = utc_naive_now()
            session.commit()
            session.refresh(row)
            return row

    def expire_due_signals(self, now: Optional[datetime] = None) -> int:
        now_value = to_utc_naive_datetime(now) if now is not None else utc_naive_now()
        with self.db.get_session() as session:
            self._assert_current_durable_lease_in_session(session, now=now_value)
            rows = session.execute(
                select(DecisionSignalRecord).where(
                    DecisionSignalRecord.status == "active",
                    DecisionSignalRecord.expires_at.is_not(None),
                    DecisionSignalRecord.expires_at <= now_value,
                )
            ).scalars().all()
            for row in rows:
                row.status = "expired"
                row.updated_at = now_value
            session.commit()
            return len(rows)

    @staticmethod
    def _current_durable_context() -> Optional[Any]:
        """Resolve the optional Worker context without coupling legacy startup."""

        try:
            from src.services.durable_job_handlers import (
                get_optional_durable_execution_context,
            )

            return get_optional_durable_execution_context()
        except (ImportError, RuntimeError):
            return None

    @classmethod
    def _assert_current_durable_lease_in_session(
        cls,
        session: Any,
        *,
        now: Optional[datetime] = None,
    ) -> None:
        """Fence every signal mutation in its own database transaction."""

        context = cls._current_durable_context()
        if context is None:
            return
        current = to_utc_naive_datetime(now) if now is not None else utc_naive_now()
        live_job_id = session.execute(
            select(AnalysisJobRecord.task_id).where(
                AnalysisJobRecord.task_id == context.job_id,
                AnalysisJobRecord.status == "processing",
                AnalysisJobRecord.lease_owner == context.worker_id,
                AnalysisJobRecord.lease_token == context.lease_token,
                AnalysisJobRecord.lease_expires_at > current,
            )
        ).scalar_one_or_none()
        if live_job_id is None:
            from src.services.durable_jobs import StaleLeaseError

            raise StaleLeaseError(
                "durable decision signal write rejected after lease loss"
            )

    @staticmethod
    def _normalize_datetime_fields(fields: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(fields)
        for field_name in ("expires_at", "created_at", "updated_at"):
            value = normalized.get(field_name)
            if isinstance(value, datetime):
                normalized[field_name] = to_utc_naive_datetime(value)
        return normalized

    @staticmethod
    def _normalize_optional_datetime(value: Optional[datetime]) -> Optional[datetime]:
        if value is None:
            return None
        return to_utc_naive_datetime(value)

    @classmethod
    def _should_refresh_existing(cls, existing: DecisionSignalRecord, fields: Dict[str, Any]) -> bool:
        expires_at = fields.get("expires_at")
        return (
            existing.status == "expired"
            and fields.get("status") == "active"
            and expires_at is not None
            and expires_at > utc_naive_now()
        )

    @classmethod
    def _refresh_existing_in_session(cls, existing: DecisionSignalRecord, fields: Dict[str, Any]) -> None:
        for field_name, value in fields.items():
            if field_name in cls._IMMUTABLE_REFRESH_FIELDS:
                continue
            setattr(existing, field_name, value)
        refreshed_at = utc_naive_now()
        # Windows wall clocks can return the same microsecond for the prior
        # status update and a subsequent refresh.  Preserve strict event order
        # so a refreshed older row can invalidate an opposing signal created
        # after its original version but before this refresh.
        prior_updated_at = existing.updated_at
        if prior_updated_at is not None:
            prior_updated_at = to_utc_naive_datetime(prior_updated_at)
            if refreshed_at <= prior_updated_at:
                from datetime import timedelta

                refreshed_at = prior_updated_at + timedelta(microseconds=1)
        existing.updated_at = refreshed_at

    @staticmethod
    def _find_existing_in_session(*, session: Any, fields: Dict[str, Any]) -> Optional[DecisionSignalRecord]:
        idempotency_key = fields.get("idempotency_key")
        if idempotency_key:
            existing = session.execute(
                select(DecisionSignalRecord)
                .where(DecisionSignalRecord.idempotency_key == idempotency_key)
                .limit(1)
            ).scalar_one_or_none()
            if existing is not None:
                DecisionSignalRepository._assert_formal_lineage_match(existing, fields)
                return existing
        source_report_id = fields.get("source_report_id")
        trace_id = fields.get("trace_id")
        source_type = fields.get("source_type")
        stock_code = fields.get("stock_code")
        market = fields.get("market")
        action = fields.get("action")
        horizon = fields.get("horizon")
        market_phase = fields.get("market_phase")
        decision_profile = fields.get("decision_profile")
        if source_report_id is not None:
            conditions = [
                DecisionSignalRecord.source_report_id == source_report_id,
                DecisionSignalRecord.source_type == source_type,
                DecisionSignalRecord.market == market,
                DecisionSignalRecord.stock_code == stock_code,
                DecisionSignalRepository._same_profile_condition(decision_profile),
                DecisionSignalRecord.action == action,
                DecisionSignalRecord.horizon == horizon,
                DecisionSignalRecord.market_phase == market_phase,
                *DecisionSignalRepository._formal_lineage_conditions(fields),
            ]
        elif trace_id:
            conditions = [
                DecisionSignalRecord.trace_id == trace_id,
                DecisionSignalRecord.source_type == source_type,
                DecisionSignalRecord.market == market,
                DecisionSignalRecord.stock_code == stock_code,
                DecisionSignalRepository._same_profile_condition(decision_profile),
                DecisionSignalRecord.action == action,
                DecisionSignalRecord.horizon == horizon,
                DecisionSignalRecord.market_phase == market_phase,
                *DecisionSignalRepository._formal_lineage_conditions(fields),
            ]
        else:
            return None
        return session.execute(
            select(DecisionSignalRecord)
            .where(and_(*conditions))
            .order_by(DecisionSignalRecord.id.asc())
            .limit(1)
        ).scalar_one_or_none()

    @classmethod
    def _find_relaxed_existing_in_session(
        cls,
        *,
        session: Any,
        fields: Dict[str, Any],
        allow_relaxed_horizon_fill: bool,
    ) -> Optional[DecisionSignalRecord]:
        source_report_id = fields.get("source_report_id")
        trace_id = fields.get("trace_id")
        if source_report_id is None and not trace_id:
            return None

        conditions = [
            DecisionSignalRecord.source_type == fields.get("source_type"),
            DecisionSignalRecord.market == fields.get("market"),
            DecisionSignalRecord.stock_code == fields.get("stock_code"),
            cls._same_profile_condition(fields.get("decision_profile")),
            DecisionSignalRecord.action == fields.get("action"),
            *cls._formal_lineage_conditions(fields),
        ]
        if source_report_id is not None:
            conditions.append(DecisionSignalRecord.source_report_id == source_report_id)
        else:
            conditions.append(DecisionSignalRecord.trace_id == trace_id)

        candidates = session.execute(
            select(DecisionSignalRecord)
            .where(and_(*conditions))
            .order_by(DecisionSignalRecord.id.asc())
        ).scalars().all()
        for candidate in candidates:
            if candidate.status not in cls._RELAXED_MERGE_STATUSES:
                continue
            if cls._can_relaxed_merge(
                candidate,
                fields,
                allow_horizon_fill=allow_relaxed_horizon_fill,
            ):
                return candidate
        return None

    @classmethod
    def _can_relaxed_merge(
        cls,
        existing: DecisionSignalRecord,
        fields: Dict[str, Any],
        *,
        allow_horizon_fill: bool,
    ) -> bool:
        new_horizon = fields.get("horizon")
        new_phase = fields.get("market_phase")

        horizon_fill = existing.horizon is None and new_horizon is not None
        if horizon_fill and not allow_horizon_fill:
            return False
        if existing.horizon is not None and existing.horizon != new_horizon:
            return False

        phase_fill = existing.market_phase is None and new_phase is not None
        if existing.market_phase is not None and existing.market_phase != new_phase:
            return False

        return horizon_fill or phase_fill

    @classmethod
    def _fill_relaxed_dimensions_in_session(
        cls,
        existing: DecisionSignalRecord,
        fields: Dict[str, Any],
        *,
        allow_horizon_fill: bool,
    ) -> bool:
        changed = False
        new_horizon = fields.get("horizon")
        new_phase = fields.get("market_phase")
        if existing.horizon is None and new_horizon is not None and allow_horizon_fill:
            existing.horizon = new_horizon
            changed = True
        if existing.market_phase is None and new_phase is not None:
            existing.market_phase = new_phase
            changed = True
        if changed:
            existing.updated_at = utc_naive_now()
        return changed

    @classmethod
    def _build_conditions(
        cls,
        *,
        stock_codes: Optional[List[str]],
        stock_identities: Optional[List[Tuple[str, str]]],
        market: Optional[str],
        action: Optional[str],
        market_phase: Optional[str],
        decision_profile_filter: DecisionProfileFilter,
        source_type: Optional[str],
        source_report_id: Optional[int],
        trace_id: Optional[str],
        trigger_source: Optional[str],
        status: Optional[str],
        created_from: Optional[datetime],
        created_to: Optional[datetime],
        expires_from: Optional[datetime],
        expires_to: Optional[datetime],
    ) -> List[Any]:
        conditions: List[Any] = []
        if stock_identities:
            identity_conditions = [
                and_(
                    DecisionSignalRecord.market == identity_market,
                    DecisionSignalRecord.stock_code == identity_code,
                )
                for identity_market, identity_code in stock_identities
            ]
            conditions.append(or_(*identity_conditions))
        elif stock_codes:
            conditions.append(DecisionSignalRecord.stock_code.in_(stock_codes))
        if market:
            conditions.append(DecisionSignalRecord.market == market)
        if action:
            conditions.append(DecisionSignalRecord.action == action)
        if market_phase:
            conditions.append(DecisionSignalRecord.market_phase == market_phase)
        cls._append_profile_filter_condition(conditions, decision_profile_filter)
        if source_type:
            conditions.append(DecisionSignalRecord.source_type == source_type)
        if source_report_id is not None:
            conditions.append(DecisionSignalRecord.source_report_id == source_report_id)
        if trace_id:
            conditions.append(DecisionSignalRecord.trace_id == trace_id)
        if trigger_source:
            conditions.append(DecisionSignalRecord.trigger_source == trigger_source)
        if status:
            conditions.append(DecisionSignalRecord.status == status)
        if created_from:
            conditions.append(DecisionSignalRecord.created_at >= created_from)
        if created_to:
            conditions.append(DecisionSignalRecord.created_at <= created_to)
        if expires_from:
            conditions.append(DecisionSignalRecord.expires_at >= expires_from)
        if expires_to:
            conditions.append(DecisionSignalRecord.expires_at <= expires_to)
        return conditions

    @staticmethod
    def _same_profile_condition(profile: Optional[str]) -> Any:
        if profile is None:
            return DecisionSignalRecord.decision_profile.is_(None)
        return DecisionSignalRecord.decision_profile == profile

    @staticmethod
    def _formal_lineage_conditions(fields: Dict[str, Any]) -> List[Any]:
        """Bind formal personal-research signals to one immutable evaluation.

        Legacy/off-only signals without a research snapshot retain their
        historical de-duplication behavior.  Once a signal is produced from a
        formal research snapshot or under shadow/enforce policy, a changed
        research/policy/portfolio evaluation must create a new DecisionSignal
        instead of mutating the prior asset.
        """

        evaluation_hash = fields.get("policy_evaluation_hash")
        if not DecisionSignalRepository._is_formal_personal_research(fields):
            return []
        return [DecisionSignalRecord.policy_evaluation_hash == evaluation_hash]

    @staticmethod
    def _assert_formal_lineage_match(
        existing: DecisionSignalRecord,
        fields: Dict[str, Any],
    ) -> None:
        incoming = fields.get("policy_evaluation_hash")
        stored = existing.policy_evaluation_hash
        if (
            not DecisionSignalRepository._is_formal_personal_research(fields)
            or incoming is None
            or stored is None
            or incoming == stored
        ):
            return
        raise ValueError(
            "idempotency_key is already bound to a different policy evaluation"
        )

    @staticmethod
    def _is_formal_personal_research(fields: Dict[str, Any]) -> bool:
        return (
            fields.get("research_snapshot_hash") is not None
            or fields.get("policy_mode") in {"shadow", "enforce"}
        )

    @classmethod
    def _append_profile_filter_condition(
        cls,
        conditions: List[Any],
        decision_profile_filter: DecisionProfileFilter,
    ) -> None:
        if decision_profile_filter.is_all:
            return
        if decision_profile_filter.is_unknown:
            conditions.append(DecisionSignalRecord.decision_profile.is_(None))
            return
        conditions.append(cls._same_profile_condition(decision_profile_filter.profile))
