# -*- coding: utf-8 -*-
"""Focused contract tests for the provider-free Decision Outcome v2 evaluator."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from src.core.decision_outcome_v2_evaluator import (
    DECISION_OUTCOME_V2_ENGINE_VERSION,
    DecisionOutcomeV2Evaluator,
    PointInTimeSw1Membership,
    normalize_final_action_family,
)


DATASET_HASH_A = "1" * 64
DATASET_HASH_B = "a" * 64


def _sessions(count: int = 20) -> list[date]:
    return [date(2025, 1, 2) + timedelta(days=index) for index in range(count)]


def _stock_bars(sessions: list[date]) -> list[dict]:
    bars = []
    for index, session in enumerate(sessions):
        bars.append({
            "trade_date": session,
            "open": 100 + index,
            "high": 104 + index,
            "low": 96 + index,
            "close": 101 + index,
            "adj_factor": 1,
        })
    return bars


def _limits(entry_date: date, *, up: float = 110, down: float = 90) -> list[dict]:
    return [{"trade_date": entry_date, "up_limit": up, "down_limit": down}]


def _benchmark_bars(sessions: list[date], *, entry: float, end: float, horizon_days: int) -> list[dict]:
    return [
        {"trade_date": sessions[0], "open": entry, "high": entry, "low": entry, "close": entry},
        {
            "trade_date": sessions[horizon_days - 1],
            "open": end,
            "high": end,
            "low": end,
            "close": end,
        },
    ]


def _evaluate(**overrides):
    sessions = overrides.pop("xshg_sessions", _sessions())
    stock_bars = overrides.pop("stock_bars", _stock_bars(sessions))
    defaults = {
        "final_action_family": "long",
        "horizon": "5d",
        "signal_session": date(2025, 1, 1),
        "xshg_sessions": sessions,
        "stock_bars": stock_bars,
        "stk_limit_rows": _limits(sessions[0]),
        "dataset_hashes": [DATASET_HASH_B, DATASET_HASH_A, DATASET_HASH_A],
    }
    defaults.update(overrides)
    return DecisionOutcomeV2Evaluator.evaluate(**defaults)


def test_long_uses_t_plus_one_raw_open_adj_factor_and_independent_benchmarks() -> None:
    sessions = _sessions()
    bars = _stock_bars(sessions)
    bars[0] = {
        "trade_date": sessions[0],
        "open": 100,
        "high": 102,
        "low": 98,
        "close": 101,
        "adj_factor": 1,
    }
    bars[4] = {
        "trade_date": sessions[4],
        "open": 50,
        "high": 60,
        "low": 45,
        "close": 55,
        "adj_factor": 2,
    }
    result = _evaluate(
        xshg_sessions=sessions,
        stock_bars=bars,
        csi300_bars=_benchmark_bars(sessions, entry=100, end=105, horizon_days=5),
        sw1_membership=PointInTimeSw1Membership(
            industry_code="801780.SI",
            industry_name="Banking",
            effective_from=date(2020, 1, 1),
            known_at=date(2024, 12, 31),
            stock_code="600000.SH",
        ),
        sw1_bars=_benchmark_bars(sessions, entry=200, end=204, horizon_days=5),
        stock_code="600000",
    )

    assert result.engine_version == DECISION_OUTCOME_V2_ENGINE_VERSION
    assert result.eval_status == "evaluated"
    assert result.terminal is True
    assert result.entry_trade_date == date(2025, 1, 2)
    assert result.entry_raw_open == 100.0
    assert result.entry_adj_factor == 1.0
    assert result.end_adjusted_close == 110.0
    assert result.stock_return_pct == pytest.approx(10.0)
    assert result.directional_return_pct == pytest.approx(10.0)
    assert result.mfe_pct == pytest.approx(20.0)
    assert result.mae_pct == pytest.approx(10.0)
    assert result.outcome == "hit"
    assert result.direction_correct is True
    assert result.csi300.code == "000300.SH"
    assert result.csi300.name == "CSI 300"
    assert result.csi300.return_pct == pytest.approx(5.0)
    assert result.csi300.stock_excess_return_pct == pytest.approx(5.0)
    assert result.csi300.directional_excess_return_pct == pytest.approx(5.0)
    assert result.sw1.code == "801780.SI"
    assert result.sw1.name == "Banking"
    assert result.sw1.return_pct == pytest.approx(2.0)
    assert result.sw1.directional_excess_return_pct == pytest.approx(8.0)
    assert result.dataset_hashes == (DATASET_HASH_A, DATASET_HASH_B)
    assert result.to_fields()["dataset_hashes"] == [DATASET_HASH_A, DATASET_HASH_B]


def test_defensive_inverts_direction_mfe_mae_and_benchmark_excess() -> None:
    sessions = _sessions()
    bars = _stock_bars(sessions)
    bars[0].update(open=100, high=102, low=98, close=101)
    bars[4].update(open=105, high=120, low=90, close=110)
    result = _evaluate(
        final_action_family="exit",
        xshg_sessions=sessions,
        stock_bars=bars,
        csi300_bars=_benchmark_bars(sessions, entry=100, end=105, horizon_days=5),
    )

    assert result.final_action_family == "defensive"
    assert result.stock_return_pct == pytest.approx(10.0)
    assert result.directional_return_pct == pytest.approx(-10.0)
    assert result.mfe_pct == pytest.approx(10.0)
    assert result.mae_pct == pytest.approx(20.0)
    assert result.outcome == "miss"
    assert result.direction_correct is False
    assert result.csi300.stock_excess_return_pct == pytest.approx(5.0)
    assert result.csi300.directional_excess_return_pct == pytest.approx(-5.0)


def test_observational_never_pretends_execution_direction_or_hit() -> None:
    sessions = _sessions()
    result = _evaluate(
        final_action_family="hold",
        xshg_sessions=sessions,
        stk_limit_rows=[],
        suspend_rows=[{"trade_date": sessions[0], "suspend_type": "S"}],
        csi300_bars=_benchmark_bars(sessions, entry=100, end=105, horizon_days=5),
    )

    assert result.final_action_family == "observational"
    assert result.eval_status == "observational"
    assert result.terminal is True
    assert result.stock_return_pct is not None
    assert result.outcome is None
    assert result.direction_correct is None
    assert result.directional_return_pct is None
    assert result.mfe_pct is None
    assert result.mae_pct is None
    assert result.csi300.stock_excess_return_pct is not None
    assert result.csi300.directional_excess_return_pct is None


def test_entry_suspension_is_terminal_unexecutable_even_without_a_stock_bar() -> None:
    sessions = _sessions()
    bars = _stock_bars(sessions)[1:]
    result = _evaluate(
        xshg_sessions=sessions,
        stock_bars=bars,
        suspend_rows=[{"trade_date": sessions[0], "suspend_type": "S"}],
    )

    assert result.eval_status == "unexecutable"
    assert result.reason == "entry_suspended"
    assert result.terminal is True
    assert result.outcome is None


@pytest.mark.parametrize(
    ("family", "limit_price", "reason"),
    [
        ("open", 110, "entry_one_price_limit_up"),
        ("reduce", 90, "entry_one_price_limit_down"),
    ],
)
def test_exact_one_price_limit_is_unexecutable_for_the_blocked_side(
    family: str,
    limit_price: float,
    reason: str,
) -> None:
    sessions = _sessions()
    bars = _stock_bars(sessions)
    bars[0] = {
        "trade_date": sessions[0],
        "open": limit_price,
        "high": limit_price,
        "low": limit_price,
        "close": limit_price,
        "adj_factor": 1,
    }
    result = _evaluate(
        final_action_family=family,
        xshg_sessions=sessions,
        stock_bars=bars,
        stk_limit_rows=_limits(sessions[0]),
    )

    assert result.eval_status == "unexecutable"
    assert result.reason == reason


def test_non_one_price_limit_bar_remains_executable() -> None:
    sessions = _sessions()
    bars = _stock_bars(sessions)
    bars[0] = {
        "trade_date": sessions[0],
        "open": 110,
        "high": 110,
        "low": 109,
        "close": 110,
        "adj_factor": 1,
    }
    result = _evaluate(xshg_sessions=sessions, stock_bars=bars)

    assert result.eval_status == "evaluated"


def test_zero_directional_return_is_a_miss_not_a_third_calibration_label() -> None:
    sessions = _sessions()
    bars = _stock_bars(sessions)
    bars[0].update(open=100, high=102, low=98, close=100)
    bars[4].update(open=100, high=103, low=97, close=100)
    result = _evaluate(xshg_sessions=sessions, stock_bars=bars)

    assert result.stock_return_pct == 0.0
    assert result.outcome == "miss"
    assert result.direction_correct is False


def test_missing_execution_limit_fails_closed() -> None:
    result = _evaluate(stk_limit_rows=[])

    assert result.eval_status == "unable"
    assert result.reason == "missing_entry_stk_limit"
    assert result.terminal is True


def test_missing_adjustment_factor_is_unable_instead_of_assuming_one() -> None:
    sessions = _sessions()
    bars = _stock_bars(sessions)
    bars[4].pop("adj_factor")
    result = _evaluate(xshg_sessions=sessions, stock_bars=bars)

    assert result.eval_status == "unable"
    assert result.reason == "invalid_window_adj_factor"


def test_unexplained_intermediate_bar_gap_is_unable_but_suspension_gap_is_allowed() -> None:
    sessions = _sessions()
    bars = [bar for index, bar in enumerate(_stock_bars(sessions)) if index != 2]

    unexplained = _evaluate(xshg_sessions=sessions, stock_bars=bars)
    suspended = _evaluate(
        xshg_sessions=sessions,
        stock_bars=bars,
        suspend_rows=[{"trade_date": sessions[2], "suspend_type": "S"}],
    )

    assert unexplained.eval_status == "unable"
    assert unexplained.reason == "missing_window_stock_bar"
    assert suspended.eval_status == "evaluated"


def test_suspended_horizon_end_is_terminal_unable_without_close_fallback() -> None:
    sessions = _sessions()
    bars = [bar for index, bar in enumerate(_stock_bars(sessions)) if index != 4]
    result = _evaluate(
        xshg_sessions=sessions,
        stock_bars=bars,
        suspend_rows=[{"trade_date": sessions[4], "suspend_type": "S"}],
    )

    assert result.eval_status == "unable"
    assert result.reason == "end_suspended_without_close"


def test_insufficient_calendar_is_pending_and_may_have_no_lineage_hashes() -> None:
    result = _evaluate(xshg_sessions=_sessions(4), stock_bars=[], dataset_hashes=[])

    assert result.eval_status == "pending"
    assert result.reason == "insufficient_xshg_sessions"
    assert result.terminal is False
    assert result.dataset_hashes == ()


def test_future_entry_session_is_pending_before_lineage_or_bars_are_required() -> None:
    sessions = _sessions()
    result = _evaluate(
        xshg_sessions=sessions,
        stock_bars=[],
        stk_limit_rows=[],
        dataset_hashes=[],
        evaluation_as_of=datetime(2025, 1, 1, 23, 0, tzinfo=timezone.utc),
    )

    assert result.eval_status == "pending"
    assert result.reason == "entry_session_not_reached"
    assert result.terminal is False
    assert result.entry_trade_date == sessions[0]
    assert result.dataset_hashes == ()


def test_entry_session_remains_pending_until_xshg_close() -> None:
    sessions = _sessions()
    result = _evaluate(
        xshg_sessions=sessions,
        stock_bars=[],
        evaluation_as_of=datetime(2025, 1, 2, 6, 59, tzinfo=timezone.utc),
    )

    assert result.eval_status == "pending"
    assert result.reason == "entry_session_not_reached"
    assert result.terminal is False


def test_future_horizon_is_pending_before_lineage_or_market_data_are_required() -> None:
    sessions = _sessions()
    result = _evaluate(
        xshg_sessions=sessions,
        stock_bars=[],
        stk_limit_rows=[],
        dataset_hashes=[],
        evaluation_as_of=datetime(2025, 1, 2, 7, 0, tzinfo=timezone.utc),
    )

    assert result.eval_status == "pending"
    assert result.reason == "horizon_not_reached"
    assert result.terminal is False
    assert result.entry_raw_open is None
    assert result.dataset_hashes == ()


def test_future_horizon_does_not_evaluate_entry_unexecutability_early() -> None:
    sessions = _sessions()
    entry_only = _stock_bars(sessions)[:1]
    entry_only[0].update(open=110, high=110, low=110, close=110)
    result = _evaluate(
        xshg_sessions=sessions,
        stock_bars=entry_only,
        evaluation_as_of=datetime(2025, 1, 2, 7, 0, tzinfo=timezone.utc),
    )

    assert result.eval_status == "pending"
    assert result.reason == "horizon_not_reached"
    assert result.terminal is False


def test_missing_entry_bar_after_xshg_close_is_terminal_unable() -> None:
    sessions = _sessions()
    result = _evaluate(
        xshg_sessions=sessions,
        stock_bars=[],
        evaluation_as_of=datetime(2025, 1, 6, 7, 0, tzinfo=timezone.utc),
    )

    assert result.eval_status == "unable"
    assert result.reason == "missing_entry_stock_bar"
    assert result.terminal is True


def test_naive_evaluation_as_of_fails_closed() -> None:
    result = _evaluate(evaluation_as_of=datetime(2025, 1, 2, 15, 0))

    assert result.eval_status == "unable"
    assert result.reason == "invalid_evaluation_as_of"


def test_terminal_calculation_without_dataset_lineage_fails_closed() -> None:
    result = _evaluate(dataset_hashes=[])

    assert result.eval_status == "unable"
    assert result.reason == "missing_dataset_hashes"


def test_invalid_dataset_lineage_fails_closed_and_is_not_partially_retained() -> None:
    result = _evaluate(dataset_hashes=[DATASET_HASH_A, "not-a-sha256"])

    assert result.eval_status == "unable"
    assert result.reason == "invalid_dataset_hashes"
    assert result.dataset_hashes == ()


def test_missing_benchmarks_keep_stock_evaluation_and_never_substitute() -> None:
    sessions = _sessions()
    result = _evaluate(
        xshg_sessions=sessions,
        csi300_bars=[],
        sw1_membership=PointInTimeSw1Membership(
            industry_code="801010.SI",
            industry_name="Agriculture",
            effective_from=date(2024, 1, 1),
            known_at=date(2025, 1, 2),
        ),
        sw1_bars=_benchmark_bars(sessions, entry=100, end=105, horizon_days=5),
    )

    assert result.eval_status == "evaluated"
    assert result.csi300.status == "unavailable"
    assert result.csi300.code == "000300.SH"
    assert result.csi300.reason == "missing_csi300_benchmark"
    assert result.csi300.return_pct is None
    assert result.sw1.status == "unavailable"
    assert result.sw1.code is None
    assert result.sw1.reason == "sw1_membership_not_known_at_signal"
    assert result.sw1.return_pct is None


def test_dataset_failure_reasons_are_preserved_per_benchmark() -> None:
    result = _evaluate(
        csi300_bars=[],
        csi300_unavailable_reason="csi300_index_daily_permission_denied",
        sw1_membership=None,
        sw1_bars=[],
        sw1_unavailable_reason="sw1_index_member_all_empty",
    )

    assert result.eval_status == "evaluated"
    assert result.csi300.code == "000300.SH"
    assert result.csi300.name == "CSI 300"
    assert result.csi300.reason == "csi300_index_daily_permission_denied"
    assert result.sw1.code is None
    assert result.sw1.name is None
    assert result.sw1.reason == "sw1_index_member_all_empty"


def test_invalid_external_benchmark_reason_is_not_persisted_verbatim() -> None:
    result = _evaluate(
        csi300_bars=[],
        csi300_unavailable_reason="Permission denied: secret detail",
    )

    assert result.csi300.reason == "invalid_csi300_unavailable_reason"


def test_invalid_point_in_time_membership_interval_is_not_treated_as_current() -> None:
    sessions = _sessions()
    result = _evaluate(
        xshg_sessions=sessions,
        sw1_membership={
            "industry_code": "801010.SI",
            "industry_name": "Agriculture",
            "effective_from": "2024-01-01",
            "effective_to": "not-a-date",
            "known_at": "2024-12-31",
        },
        sw1_bars=_benchmark_bars(sessions, entry=100, end=105, horizon_days=5),
    )

    assert result.sw1.status == "unavailable"
    assert result.sw1.reason == "invalid_point_in_time_sw1_membership"


@pytest.mark.parametrize(
    "membership",
    [
        PointInTimeSw1Membership(
            industry_code="801010.SI",
            industry_name=None,
            effective_from=date(2024, 1, 1),
            known_at=date(2024, 12, 31),
        ),
        PointInTimeSw1Membership(
            industry_code="not-sw1",
            industry_name="Agriculture",
            effective_from=date(2024, 1, 1),
            known_at=date(2024, 12, 31),
        ),
        PointInTimeSw1Membership(
            industry_code="801010.SI",
            industry_name="Agriculture",
            effective_from=date(2024, 1, 1),
            known_at=None,
        ),
    ],
)
def test_incomplete_point_in_time_membership_is_unavailable(membership) -> None:
    result = _evaluate(sw1_membership=membership)

    assert result.sw1.status == "unavailable"
    assert result.sw1.reason == "invalid_point_in_time_sw1_membership"


def test_membership_known_at_uses_xshg_calendar_date_not_utc_date() -> None:
    sessions = _sessions()
    result = _evaluate(
        xshg_sessions=sessions,
        sw1_membership=PointInTimeSw1Membership(
            industry_code="801010.SI",
            industry_name="Agriculture",
            effective_from=date(2024, 1, 1),
            # UTC Jan 1 is already Jan 2 in the XSHG timezone.
            known_at=datetime(2025, 1, 1, 16, 30, tzinfo=timezone.utc),
        ),
        sw1_bars=_benchmark_bars(sessions, entry=100, end=105, horizon_days=5),
    )

    assert result.sw1.status == "unavailable"
    assert result.sw1.reason == "sw1_membership_not_known_at_signal"


def test_missing_industry_end_bar_has_independent_explicit_reason() -> None:
    sessions = _sessions()
    result = _evaluate(
        xshg_sessions=sessions,
        csi300_bars=_benchmark_bars(sessions, entry=100, end=105, horizon_days=5),
        sw1_membership=PointInTimeSw1Membership(
            industry_code="801010.SI",
            industry_name="Agriculture",
            effective_from=date(2024, 1, 1),
            known_at=date(2024, 12, 31),
        ),
        sw1_bars=[{"trade_date": sessions[0], "open": 100, "high": 100, "low": 100, "close": 100}],
    )

    assert result.csi300.status == "available"
    assert result.sw1.status == "unavailable"
    assert result.sw1.code == "801010.SI"
    assert result.sw1.name == "Agriculture"
    assert result.sw1.reason == "missing_sw1_end_bar"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("open", "long"),
        ("add", "long"),
        ("open_candidate", "long"),
        ("add_candidate", "long"),
        ("reduce", "defensive"),
        ("exit", "defensive"),
        ("reduce_candidate", "defensive"),
        ("exit_candidate", "defensive"),
        ("observe", "observational"),
        ("hold", "observational"),
    ],
)
def test_final_action_family_contract(value: str, expected: str) -> None:
    assert normalize_final_action_family(value) == expected


@pytest.mark.parametrize("value", ["maybe", "buy", "sell", "watch", "alert"])
def test_invalid_action_family_is_rejected_instead_of_guessed(value: str) -> None:
    with pytest.raises(ValueError, match="final_action_family"):
        normalize_final_action_family(value)
