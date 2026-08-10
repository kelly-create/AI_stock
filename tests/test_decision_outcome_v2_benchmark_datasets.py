"""Focused offline tests for immutable Decision Outcome v2 benchmarks."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path

import pandas as pd
import pytest

from data_provider.tushare_provider import TusharePermissionError
from src.services.research.availability import (
    DATASET_DEFINITIONS,
    DEFAULT_RESEARCH_DATASETS,
)
from src.services.research.collector import (
    ResearchDatasetCollector,
    _reset_collector_state_for_tests,
)
from src.services.research.decision_outcome_v2_datasets import (
    CSI300_INDEX_DAILY_DATASET,
    DECISION_OUTCOME_V2_BENCHMARK_DATASETS,
    DECISION_OUTCOME_V2_REQUIRED_FIELDS,
    INDEX_DAILY_FIELDS,
    SW1_INDEX_CLASSIFY_DATASET,
    SW1_INDEX_CLASSIFY_FIELDS,
    SW1_INDEX_DAILY_DATASET,
    SW1_INDEX_DAILY_FIELDS,
    SW1_INDEX_MEMBER_ALL_DATASET,
    SW1_INDEX_MEMBER_ALL_FIELDS,
    DecisionOutcomeV2DatasetQuery,
    Sw1MembershipResolutionV2,
    TushareDatasetRequestV2,
    benchmark_dataset_unavailable_reason,
    build_csi300_index_daily_query,
    build_sw1_index_classify_query,
    build_sw1_index_daily_query,
    build_sw1_index_member_all_query,
    resolve_sw1_membership,
    validate_decision_outcome_v2_frames,
)
from src.services.research.raw_store import RawArtifactStore
from src.services.research.repositories import LeaseFence, SnapshotWriteResult


AS_OF = datetime(2026, 8, 10, 9, 0, tzinfo=timezone.utc)
LEASE = LeaseFence("outcome-v2-job", "worker-1", "lease-1")
HASH_A = "a" * 64
HASH_B = "b" * 64


@pytest.fixture(autouse=True)
def _reset_collector_cache():
    _reset_collector_state_for_tests()
    yield
    _reset_collector_state_for_tests()


class _Provider:
    def __init__(self, responses) -> None:
        self.responses = dict(responses)
        self.calls: list[tuple[str, str, dict]] = []

    def query(self, api_name, fields="", **params):
        params.pop("_cancel_event", None)
        self.calls.append((api_name, fields, dict(params)))
        response = self.responses[api_name]
        if callable(response):
            response = response(fields=fields, **params)
        if isinstance(response, BaseException):
            raise response
        return response.copy(deep=True)


class _Repository:
    def __init__(self) -> None:
        self.inputs = []

    def get_job_dataset(self, **_kwargs):
        return None

    def list_dataset_checkpoints(self, **_kwargs):
        return []

    def assert_live_lease(self, _lease, *, now=None):
        del now

    def write_dataset(self, snapshot, *, lease, establish_reference_for=None):
        del lease, establish_reference_for
        self.inputs.append(snapshot)
        index = len(self.inputs)
        return SnapshotWriteResult(index, f"{index:064x}", True)


def _collector(tmp_path: Path, responses):
    provider = _Provider(responses)
    repository = _Repository()
    collector = ResearchDatasetCollector(
        provider,
        repository,
        RawArtifactStore(tmp_path / "raw"),
        clock=lambda: AS_OF + timedelta(minutes=1),
    )
    return collector, provider, repository


def _daily_row(*, code="000300.SH", trade_date="20260811", name=None):
    row = {
        "ts_code": code,
        "trade_date": trade_date,
        "close": 101.0,
        "open": 100.0,
        "high": 102.0,
        "low": 99.0,
        "pre_close": 99.0,
        "change": 2.0,
        "pct_chg": 2.02,
        "vol": 10.0,
        "amount": 20.0,
    }
    if code.endswith(".SI"):
        row = {
            "ts_code": code,
            "trade_date": trade_date,
            "name": name or "银行",
            "open": 100.0,
            "low": 99.0,
            "high": 102.0,
            "close": 101.0,
            "change": 2.0,
            "pct_change": 2.02,
            "vol": 10.0,
            "amount": 20.0,
            "pe": 7.0,
            "pb": 0.8,
            "float_mv": 1000.0,
            "total_mv": 1200.0,
        }
    return row


def _member_row(
    *,
    in_date="20210101",
    out_date=None,
    is_new="Y",
    code="801780.SI",
    name="银行",
):
    return {
        "l1_code": code,
        "l1_name": name,
        "l2_code": "801190.SI",
        "l2_name": "银行Ⅱ",
        "l3_code": "851911.SI",
        "l3_name": "国有大型银行Ⅲ",
        "ts_code": "600519.SH",
        "name": "贵州茅台",
        "in_date": in_date,
        "out_date": out_date,
        "is_new": is_new,
    }


def _classify_row(*, code="801780.SI", name="银行", is_pub="1"):
    return {
        "index_code": code,
        "industry_name": name,
        "parent_code": "0",
        "level": "L1",
        "industry_code": "480000",
        "is_pub": is_pub,
        "src": "SW2021",
    }


def _resolved_membership(**overrides):
    values = {
        "stock_code": "600519",
        "decision_date": date(2026, 8, 10),
        "decision_known_at": datetime(
            2026, 8, 10, 7, 0, tzinfo=timezone.utc
        ),
        "member_rows": [_member_row()],
        "classify_rows": [_classify_row()],
        "member_known_at": datetime(2026, 8, 10, 6, 0, tzinfo=timezone.utc),
        "classify_known_at": datetime(2026, 8, 10, 6, 1, tzinfo=timezone.utc),
        "member_snapshot_hash": HASH_A,
        "classify_snapshot_hash": HASH_B,
    }
    values.update(overrides)
    return resolve_sw1_membership(**values)


def test_benchmark_registry_is_additive_and_does_not_expand_default_bundle() -> None:
    assert DECISION_OUTCOME_V2_BENCHMARK_DATASETS == (
        "csi300_index_daily",
        "sw1_index_classify",
        "sw1_index_member_all",
        "sw1_index_daily",
    )
    assert DECISION_OUTCOME_V2_BENCHMARK_DATASETS == tuple(
        name for name in DATASET_DEFINITIONS if name not in DEFAULT_RESEARCH_DATASETS
    )
    for dataset in DECISION_OUTCOME_V2_BENCHMARK_DATASETS:
        definition = DATASET_DEFINITIONS[dataset]
        assert definition.requires_query_plan is True
        assert definition.required_fields == DECISION_OUTCOME_V2_REQUIRED_FIELDS[dataset]
    assert "csi300_index_daily" not in DEFAULT_RESEARCH_DATASETS


def test_query_plans_use_official_endpoints_fields_and_exact_windows() -> None:
    csi = build_csi300_index_daily_query(date(2026, 8, 11), date(2026, 9, 7))
    assert csi.dataset == CSI300_INDEX_DAILY_DATASET
    assert csi.requests[0].api_name == "index_daily"
    assert csi.requests[0].fields == INDEX_DAILY_FIELDS
    assert dict(csi.requests[0].params) == {
        "end_date": "20260907",
        "start_date": "20260811",
        "ts_code": "000300.SH",
    }

    classify = build_sw1_index_classify_query()
    assert classify.requests[0].api_name == "index_classify"
    assert classify.requests[0].fields == SW1_INDEX_CLASSIFY_FIELDS
    assert dict(classify.requests[0].params) == {"level": "L1", "src": "SW2021"}

    member = build_sw1_index_member_all_query("600519")
    assert member.requests[0].fields == SW1_INDEX_MEMBER_ALL_FIELDS
    assert [dict(item.params)["is_new"] for item in member.requests] == ["Y", "N"]
    assert all(dict(item.params)["ts_code"] == "600519.SH" for item in member.requests)

    with pytest.raises(ValueError, match="cannot be after"):
        build_csi300_index_daily_query(date(2026, 8, 12), date(2026, 8, 11))

    with pytest.raises(ValueError, match="CSI300 query parameters"):
        DecisionOutcomeV2DatasetQuery(
            dataset=CSI300_INDEX_DAILY_DATASET,
            requests=(
                TushareDatasetRequestV2(
                    api_name="index_daily",
                    fields=INDEX_DAILY_FIELDS,
                    params=(
                        ("ts_code", "399300.SZ"),
                        ("start_date", "20260811"),
                        ("end_date", "20260907"),
                    ),
                ),
            ),
            start_date=date(2026, 8, 11),
            end_date=date(2026, 9, 7),
        )


def test_membership_resolution_and_sw_daily_require_frozen_l1_code() -> None:
    resolution = _resolved_membership()

    assert resolution.status == "available"
    assert resolution.industry_code == "801780.SI"
    assert resolution.industry_name == "银行"
    assert resolution.dataset_hashes == (HASH_A, HASH_B)
    assert resolution.to_evaluator_mapping()["stock_code"] == "600519.SH"

    query = build_sw1_index_daily_query(
        resolution,
        date(2026, 8, 11),
        date(2026, 9, 7),
    )
    assert query.dataset == SW1_INDEX_DAILY_DATASET
    assert query.requests[0].api_name == "sw_daily"
    assert query.requests[0].fields == SW1_INDEX_DAILY_FIELDS
    assert dict(query.requests[0].params) == {
        "end_date": "20260907",
        "start_date": "20260811",
        "ts_code": "801780.SI",
    }
    assert query.source_dataset_hashes == (HASH_A, HASH_B)

    with pytest.raises(ValueError, match="frozen SW1"):
        build_sw1_index_daily_query(
            Sw1MembershipResolutionV2(status="unavailable", reason="missing"),
            date(2026, 8, 11),
            date(2026, 9, 7),
        )


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        (
            {
                "member_rows": [
                    _member_row(in_date="20210101"),
                    _member_row(in_date="20220101", code="801010.SI", name="农林牧渔"),
                ]
            },
            "overlapping_sw1_membership_intervals",
        ),
        (
            {"member_rows": []},
            "missing_sw1_membership_at_decision",
        ),
        (
            {"classify_rows": []},
            "missing_sw1_classification",
        ),
        (
            {
                "member_known_at": datetime(
                    2026, 8, 10, 7, 1, tzinfo=timezone.utc
                )
            },
            "sw1_membership_not_known_at_decision",
        ),
        (
            {"classify_rows": [_classify_row(name="非银金融")]},
            "sw1_industry_name_mismatch",
        ),
    ],
)
def test_membership_resolution_fails_closed(overrides, reason) -> None:
    result = _resolved_membership(**overrides)

    assert result.status == "unavailable"
    assert result.reason == reason
    assert result.to_evaluator_mapping() is None


def test_frame_schema_rejects_wrong_series_window_duplicate_and_zero() -> None:
    query = build_csi300_index_daily_query(date(2026, 8, 11), date(2026, 9, 7))
    valid = pd.DataFrame([_daily_row()])
    assert validate_decision_outcome_v2_frames(query, [valid]).to_dict("records") == [
        _daily_row()
    ]

    wrong_code = valid.copy()
    wrong_code.loc[0, "ts_code"] = "399300.SZ"
    with pytest.raises(ValueError, match="requested ts_code"):
        validate_decision_outcome_v2_frames(query, [wrong_code])

    outside = valid.copy()
    outside.loc[0, "trade_date"] = "20260908"
    with pytest.raises(ValueError, match="outside"):
        validate_decision_outcome_v2_frames(query, [outside])

    duplicate = pd.concat([valid, valid], ignore_index=True)
    with pytest.raises(ValueError, match="duplicate sessions"):
        validate_decision_outcome_v2_frames(query, [duplicate])

    zero = valid.copy()
    zero.loc[0, "open"] = 0
    with pytest.raises(ValueError, match="must be positive"):
        validate_decision_outcome_v2_frames(query, [zero])


def test_collector_executes_exact_plan_and_persists_stock_scoped_rows(tmp_path: Path) -> None:
    plan = build_csi300_index_daily_query(date(2026, 8, 11), date(2026, 9, 7))
    collector, provider, repository = _collector(
        tmp_path,
        {
            "index_daily": pd.DataFrame(
                [_daily_row(), _daily_row(trade_date="20260907")]
            )
        },
    )

    result = collector.collect_dataset(
        "600519",
        CSI300_INDEX_DAILY_DATASET,
        as_of=datetime(2026, 9, 7, 8, 0, tzinfo=timezone.utc),
        lease=LEASE,
        query_plan=plan,
    )

    assert result.status == "available"
    assert result.row_count == 2
    assert provider.calls == [
        (
            "index_daily",
            ",".join(INDEX_DAILY_FIELDS),
            {
                "end_date": "20260907",
                "start_date": "20260811",
                "ts_code": "000300.SH",
            },
        )
    ]
    assert repository.inputs[0].dataset == CSI300_INDEX_DAILY_DATASET
    assert repository.inputs[0].scope_type == "stock"
    assert repository.inputs[0].scope_value == "600519"
    raw = json.loads(collector.raw_store.read(result.raw_ref).decode("utf-8"))
    assert raw["requests"][0]["api_name"] == "index_daily"
    assert raw["params"]["requests"][0]["params"]["ts_code"] == "000300.SH"


def test_member_collector_uses_both_membership_states_and_combines_rows(tmp_path: Path) -> None:
    def response(*, is_new, **_kwargs):
        return pd.DataFrame(
            [
                _member_row(
                    in_date=("20210101" if is_new == "Y" else "20100101"),
                    out_date=(None if is_new == "Y" else "20210101"),
                    is_new=is_new,
                )
            ],
            columns=SW1_INDEX_MEMBER_ALL_FIELDS,
        )

    collector, provider, _repository = _collector(
        tmp_path,
        {"index_member_all": response},
    )
    result = collector.collect_dataset(
        "600519",
        SW1_INDEX_MEMBER_ALL_DATASET,
        as_of=AS_OF,
        lease=LEASE,
        reference_mode="live",
        query_plan=build_sw1_index_member_all_query("600519"),
    )

    assert result.status == "available"
    assert result.row_count == 2
    assert [call[2]["is_new"] for call in provider.calls] == ["Y", "N"]


def test_historical_membership_never_backfills_from_provider(tmp_path: Path) -> None:
    collector, provider, _repository = _collector(
        tmp_path,
        {"index_member_all": pd.DataFrame(columns=SW1_INDEX_MEMBER_ALL_FIELDS)},
    )

    result = collector.collect_dataset(
        "600519",
        SW1_INDEX_MEMBER_ALL_DATASET,
        as_of=AS_OF,
        lease=LEASE,
        reference_mode="historical",
        query_plan=build_sw1_index_member_all_query("600519"),
    )

    assert result.status == "empty"
    assert result.reused is True
    assert provider.calls == []


@pytest.mark.parametrize(
    ("response", "expected_status", "expected_error", "expected_reason"),
    [
        (
            TusharePermissionError("no index_daily entitlement"),
            "permission_denied",
            "permission_denied",
            "csi300_index_daily_permission_denied",
        ),
        (
            pd.DataFrame(columns=INDEX_DAILY_FIELDS),
            "empty",
            None,
            "csi300_index_daily_empty",
        ),
        (
            pd.DataFrame([_daily_row()]).drop(columns=["open"]),
            "fetch_failed",
            "response_schema_error",
            "csi300_index_daily_schema_drift",
        ),
    ],
)
def test_permission_empty_and_schema_drift_remain_distinct(
    tmp_path: Path,
    response,
    expected_status,
    expected_error,
    expected_reason,
) -> None:
    collector, _provider, _repository = _collector(
        tmp_path,
        {"index_daily": response},
    )
    result = collector.collect_dataset(
        "600519",
        CSI300_INDEX_DAILY_DATASET,
        as_of=datetime(2026, 9, 7, 8, 0, tzinfo=timezone.utc),
        lease=LEASE,
        query_plan=build_csi300_index_daily_query(
            date(2026, 8, 11), date(2026, 9, 7)
        ),
    )

    assert result.status == expected_status
    assert result.error_code == expected_error
    assert (
        benchmark_dataset_unavailable_reason(
            result.dataset,
            result.status,
            result.error_code,
        )
        == expected_reason
    )


def test_benchmark_dataset_cannot_use_legacy_approximate_window(tmp_path: Path) -> None:
    collector, _provider, _repository = _collector(
        tmp_path,
        {"index_daily": pd.DataFrame([_daily_row()])},
    )

    with pytest.raises(ValueError, match="requires an exact query_plan"):
        collector.collect_dataset(
            "600519",
            CSI300_INDEX_DAILY_DATASET,
            as_of=AS_OF,
            lease=LEASE,
        )


def test_authentication_and_entitlement_reasons_are_not_collapsed() -> None:
    assert benchmark_dataset_unavailable_reason(
        CSI300_INDEX_DAILY_DATASET,
        "permission_denied",
        "authentication_failed",
    ) == "csi300_index_daily_authentication_failed"
    assert benchmark_dataset_unavailable_reason(
        CSI300_INDEX_DAILY_DATASET,
        "permission_denied",
        "permission_denied",
    ) == "csi300_index_daily_permission_denied"


def test_schema_constants_match_official_endpoint_names() -> None:
    assert DATASET_DEFINITIONS[CSI300_INDEX_DAILY_DATASET].provider_api_name == "index_daily"
    assert DATASET_DEFINITIONS[SW1_INDEX_CLASSIFY_DATASET].provider_api_name == "index_classify"
    assert DATASET_DEFINITIONS[SW1_INDEX_MEMBER_ALL_DATASET].provider_api_name == "index_member_all"
    assert DATASET_DEFINITIONS[SW1_INDEX_DAILY_DATASET].provider_api_name == "sw_daily"
