from __future__ import annotations

from datetime import date

import pytest

from src.config import Config
from src.services.personal_research_policy_context_service import (
    POLICY_CONTEXT_CONTRACT_VERSION,
    PersonalResearchPolicyContextService,
)
from src.services.portfolio_policy_gate_service import PortfolioPolicyGateService


class _PortfolioService:
    def get_policy_valuation_state(self, *, account_id, as_of):
        assert account_id == 7
        assert as_of == date(2026, 8, 10)
        return {
            "account_id": 7,
            "as_of": "2026-08-10",
            "total_equity": 100_000.0,
            "fx_stale": False,
            "data_quality": "ok",
            "limitations": [],
            "positions": [
                {
                    "symbol": "600519",
                    "market": "cn",
                    "quantity": 10,
                    "market_value_base": 10_000.0,
                    "price_available": True,
                    "price_stale": False,
                    "price_date": "2026-08-07",
                },
                {
                    "symbol": "000001",
                    "market": "cn",
                    "quantity": 100,
                    "market_value_base": 20_000.0,
                    "price_available": True,
                    "price_stale": False,
                    "price_date": "2026-08-07",
                },
            ],
        }


class _ResearchRepository:
    def list_datasets(self, *, scope_value, dataset, as_of, limit):
        del as_of, limit
        assert dataset == "stock_basic"
        code = "600519" if "600519" in scope_value else "000001"
        return [
            {
                "id": 2 if code == "600519" else 1,
                "available_at": "2026-08-10T00:00:00Z",
                "status": "available",
                "content_hash": ("a" if code == "600519" else "b") * 64,
                "normalized": [
                    {
                        "ts_code": f"{code}.{'SH' if code == '600519' else 'SZ'}",
                        "industry": "食品饮料",
                    }
                ],
            }
        ]


def _service() -> PersonalResearchPolicyContextService:
    return PersonalResearchPolicyContextService(
        db_manager=object(),
        portfolio_service=_PortfolioService(),
        research_repository=_ResearchRepository(),
    )


def test_builds_complete_replayable_server_side_policy_context() -> None:
    context = _service().build(
        account_id=7,
        stock_code="600519",
        target_weight_pct=5,
        entry_price=100,
        stop_loss=90,
        as_of=date(2026, 8, 10),
        decision_session_date=date(2026, 8, 11),
    )

    assert context["portfolio_complete"] is True
    assert context["incomplete_reason_codes"] == []
    assert context["current_position_weight_pct"] == 10.0
    assert context["projected_position_weight_pct"] == 5.0
    assert context["projected_sector_weight_pct"] == 25.0
    assert context["position_risk_pct"] == 0.5
    assert context["portfolio_snapshot_ref"] == (
        f"{POLICY_CONTEXT_CONTRACT_VERSION}:{context['audit_snapshot_hash']}"
    )
    assert context["audit_snapshot"]["positions"][0]["stock_code"] == "000001"
    assert context["audit_snapshot"]["decision_session_date"] == "2026-08-11"
    assert context["audit_snapshot"]["valuation_bar_date"] == "2026-08-10"
    assert context["audit_snapshot"]["sector_dataset_hashes"] == [
        "a" * 64,
        "b" * 64,
    ]


def test_missing_server_facts_are_explicit_and_never_coerced_to_zero() -> None:
    context = _service().build(
        account_id=None,
        stock_code="600519",
        target_weight_pct=None,
        entry_price=None,
        stop_loss=None,
        as_of=date(2026, 8, 10),
    )

    assert context["portfolio_complete"] is False
    assert set(context["incomplete_reason_codes"]) >= {
        "policy_account_missing",
        "target_weight_missing",
        "entry_price_missing",
        "stop_loss_missing",
    }
    assert context["current_position_weight_pct"] is None
    assert context["projected_position_weight_pct"] is None
    assert context["projected_sector_weight_pct"] is None
    assert context["position_risk_pct"] is None


def test_position_identity_uses_market_and_stock_code_together() -> None:
    class MixedMarketPortfolioService(_PortfolioService):
        def get_policy_valuation_state(self, *, account_id, as_of):
            snapshot = super().get_policy_valuation_state(
                account_id=account_id,
                as_of=as_of,
            )
            snapshot["positions"] = [
                *snapshot["positions"],
                {
                    "symbol": "600519",
                    "market": "us",
                    "quantity": 3,
                    "market_value_base": 30_000.0,
                    "price_available": True,
                    "price_stale": False,
                    "price_date": "2026-08-07",
                },
            ]
            return snapshot

    service = PersonalResearchPolicyContextService(
        db_manager=object(),
        portfolio_service=MixedMarketPortfolioService(),
        research_repository=_ResearchRepository(),
    )
    context = service.build(
        account_id=7,
        stock_code="600519",
        target_weight_pct=5,
        entry_price=100,
        stop_loss=90,
        as_of=date(2026, 8, 10),
    )

    assert context["current_position_weight_pct"] == 10.0
    assert context["projected_sector_weight_pct"] == 55.0


def test_gate_binds_full_context_and_rejects_audit_tampering() -> None:
    context = _service().build(
        account_id=7,
        stock_code="600519",
        target_weight_pct=5,
        entry_price=100,
        stop_loss=90,
        as_of=date(2026, 8, 10),
    )
    config = Config(
        stock_list=["600519"],
        personal_research_enabled=True,
        research_factors_enabled=True,
        research_evidence_enabled=True,
        portfolio_policy_gate_mode="shadow",
    )
    service = PortfolioPolicyGateService(config=config)
    payload = {
        "research_stance": "bullish",
        "account_action": "open_candidate",
        "value_quality_score": 75,
        "trend_timing_score": 75,
        "catalyst_score": 70,
        "risk_score": 30,
        "evidence_quality_score": 80,
        "policy_context": context,
    }
    normalized = {
        "stock_code": "600519",
        "market": "cn",
        "action": "buy",
        "research_snapshot_hash": "c" * 64,
    }

    result = service.evaluate(payload=payload, normalized_signal=normalized)
    assert result.evaluation_fields["portfolio_context_json"]

    tampered = {
        **context,
        "audit_snapshot": {
            **context["audit_snapshot"],
            "target_weight_pct": 15.0,
        },
    }
    with pytest.raises(ValueError, match="audit snapshot hash mismatch"):
        service.evaluate(
            payload={**payload, "policy_context": tampered},
            normalized_signal=normalized,
        )
