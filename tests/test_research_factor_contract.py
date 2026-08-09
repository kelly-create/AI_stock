from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.services.research.factor_contract import (
    MAX_FACTOR_DATASET_HASHES,
    MAX_FACTOR_KEY_CHARS,
    MAX_FACTOR_MAPPING_ITEMS,
    MAX_FACTOR_STRING_CHARS,
    MAX_FACTOR_TREE_DEPTH,
    MAX_FACTOR_UNKNOWNS,
    validate_factor_contract,
    validate_factor_dataset_hashes,
    validate_factor_payload,
    validate_factor_unknowns,
)
from src.services.research.factor_service import evaluate_research_factors


AS_OF = datetime(2025, 6, 30, 10, 0, tzinfo=timezone.utc)
HASH_A = "a" * 64
HASH_B = "b" * 64


def _payload(**overrides):
    payload = {
        "stock_code": "600519",
        "as_of": AS_OF.isoformat().replace("+00:00", "Z"),
        "profile": {
            "profile": "industrial",
            "source": "comp_type",
            "resolver_version": "profile-resolver-v1",
        },
        "value": {"status": "available", "score": 72.0, "metrics": []},
        "quality": {"status": "available", "score": 80.0, "metrics": []},
        "trend_timing": {"status": "available", "score": 68.0, "metrics": []},
        "catalyst": {"status": "available", "score": 55.0, "metrics": []},
        "risk": {"status": "available", "score": 20.0, "metrics": []},
        "policy_version": "factor-policy-v1",
    }
    payload.update(overrides)
    return payload


def _unknown(component="quality", metric="roe_5y", reason="field_missing"):
    return {"component": component, "metric": metric, "reason": reason}


def _contract_kwargs(**overrides):
    values = {
        "factor_payload": _payload(),
        "unknowns": [],
        "input_dataset_hashes": [HASH_A],
        "as_of": AS_OF,
        "available_at": AS_OF - timedelta(minutes=1),
        "value_score": 72.0,
        "quality_score": 80.0,
        "trend_score": 68.0,
        "catalyst_score": 55.0,
        "risk_penalty": 20.0,
    }
    values.update(overrides)
    return values


def test_writer_contract_normalizes_containers_unknowns_and_dataset_hashes():
    contract = validate_factor_contract(
        **_contract_kwargs(
            unknowns=(
                _unknown("risk", "volatility_60d", "field_missing"),
                _unknown(),
                _unknown(),
            ),
            input_dataset_hashes=(HASH_B, HASH_A, HASH_B),
        )
    )

    assert contract.input_dataset_hashes == (HASH_A, HASH_B)
    assert [dict(item) for item in contract.unknowns] == [
        _unknown(),
        _unknown("risk", "volatility_60d", "field_missing"),
    ]
    with pytest.raises(TypeError):
        contract.factor_payload["value"] = {}


def test_read_and_api_mode_requires_canonical_references():
    with pytest.raises(ValueError, match="sorted and unique"):
        validate_factor_contract(
            **_contract_kwargs(input_dataset_hashes=[HASH_B, HASH_A]),
            require_canonical_references=True,
        )
    with pytest.raises(ValueError, match="sorted, unique, and normalized"):
        validate_factor_contract(
            **_contract_kwargs(
                unknowns=[
                    _unknown("risk", "volatility_60d", "field_missing"),
                    _unknown(),
                ]
            ),
            require_canonical_references=True,
        )

    accepted = validate_factor_contract(
        **_contract_kwargs(
            unknowns=[
                _unknown(),
                _unknown("risk", "volatility_60d", "field_missing"),
            ],
            input_dataset_hashes=[HASH_A, HASH_B],
        ),
        require_canonical_references=True,
    )
    assert accepted.input_dataset_hashes == (HASH_A, HASH_B)


def test_existing_research_factor_result_to_dict_is_contract_compatible():
    result = evaluate_research_factors(
        {
            "stock_code": "600519",
            "as_of": AS_OF,
            "company": {},
            "fundamentals": {},
            "bars": [],
            "catalysts": {},
            "market_state": {},
        }
    )
    payload = result.to_dict()
    contract = validate_factor_contract(
        factor_payload=result,
        unknowns=[],
        input_dataset_hashes=[],
        as_of=AS_OF,
        available_at=AS_OF,
        value_score=result.value.score,
        quality_score=result.quality.score,
        trend_score=result.trend_timing.score,
        catalyst_score=result.catalyst.score,
        risk_penalty=result.risk.score,
        require_canonical_references=True,
    )

    assert dict(contract.factor_payload)["stock_code"] == payload["stock_code"]
    assert dict(contract.factor_payload)["as_of"] == payload["as_of"]


@pytest.mark.parametrize(
    ("field", "component", "summary"),
    [
        ("value_score", "value", 71.0),
        ("quality_score", "quality", None),
        ("trend_score", "trend_timing", 99.0),
        ("catalyst_score", "catalyst", 0.0),
        ("risk_penalty", "risk", 21.0),
    ],
)
def test_summary_scores_must_equal_their_payload_component(field, component, summary):
    kwargs = _contract_kwargs(**{field: summary})
    with pytest.raises(ValueError, match=field):
        validate_factor_contract(**kwargs)

    payload = _payload()
    del payload[component]
    with pytest.raises(ValueError, match=field):
        validate_factor_contract(
            **_contract_kwargs(factor_payload=payload, **{field: summary or 1.0})
        )


def test_missing_component_only_allows_a_null_summary():
    payload = _payload()
    del payload["quality"]

    accepted = validate_factor_contract(
        **_contract_kwargs(factor_payload=payload, quality_score=None)
    )
    assert "quality" not in accepted.factor_payload


@pytest.mark.parametrize("score", [True, float("nan"), float("inf"), -1.0, 101.0])
def test_factor_scores_reject_booleans_non_finite_and_out_of_range(score):
    payload = _payload()
    payload["value"] = {"score": score}
    error = TypeError if score is True else ValueError
    with pytest.raises(error, match="score"):
        validate_factor_contract(
            **_contract_kwargs(factor_payload=payload, value_score=score)
        )


def test_as_of_is_the_upper_bound_for_availability_and_nested_observation_times():
    with pytest.raises(ValueError, match="available_at cannot be after as_of"):
        validate_factor_contract(
            **_contract_kwargs(available_at=AS_OF + timedelta(microseconds=1))
        )

    mismatched = _payload(as_of=(AS_OF - timedelta(seconds=1)).isoformat())
    with pytest.raises(ValueError, match="must match the outer as_of"):
        validate_factor_contract(**_contract_kwargs(factor_payload=mismatched))

    future = _payload(
        audit={"published_at": (AS_OF + timedelta(seconds=1)).isoformat()}
    )
    with pytest.raises(ValueError, match="cannot be after as_of"):
        validate_factor_contract(**_contract_kwargs(factor_payload=future))


@pytest.mark.parametrize(
    "time_key",
    [
        "availableAt",
        "announcementDate",
        "annDate",
        "ann_date",
        "fAnnDate",
        "f_ann_date",
        "impAnnDate",
        "imp_ann_date",
        "actualAnnDate",
        "announcedAt",
        "publishedAt",
        "publicationDate",
        "publish_date",
        "pubDate",
        "dataAsOf",
        "knowledgeAsOf",
        "observedAt",
        "providerTimestamp",
        "fetchedAt",
        "tradeDate",
    ],
)
def test_snake_camel_and_provider_time_aliases_cannot_bypass_as_of(time_key):
    payload = _payload(
        audit={time_key: (AS_OF + timedelta(microseconds=1)).isoformat()}
    )

    with pytest.raises(ValueError, match=rf"audit\.{time_key} cannot be after as_of"):
        validate_factor_contract(**_contract_kwargs(factor_payload=payload))


@pytest.mark.parametrize(
    "time_key",
    [
        "availableAt",
        "announcementDate",
        "annDate",
        "ann_date",
        "fAnnDate",
        "f_ann_date",
        "impAnnDate",
        "imp_ann_date",
        "publishedAt",
        "dataAsOf",
        "knowledgeAsOf",
        "observedAt",
        "providerTimestamp",
        "fetchedAt",
    ],
)
def test_snake_camel_and_provider_time_aliases_accept_legal_prior_values(time_key):
    payload = _payload(audit={time_key: "20250629"})

    contract = validate_factor_contract(
        **_contract_kwargs(factor_payload=payload),
        require_canonical_references=True,
    )

    assert contract.factor_payload["audit"][time_key] == "20250629"


def test_root_camel_as_of_alias_must_match_the_outer_boundary():
    payload = _payload()
    del payload["as_of"]
    payload["asOf"] = (AS_OF - timedelta(seconds=1)).isoformat()

    with pytest.raises(ValueError, match="must match the outer as_of"):
        validate_factor_contract(**_contract_kwargs(factor_payload=payload))

    payload["asOf"] = AS_OF.isoformat()
    validate_factor_contract(**_contract_kwargs(factor_payload=payload))


@pytest.mark.parametrize(
    "payload",
    [
        {"nested": {"note": "sk-" + "a" * 20}},
        {"nested": {"api_key": "not-a-real-key"}},
        {"nested": {"note": "authorization=private-value"}},
    ],
)
def test_factor_payload_recursively_rejects_public_secret_material(payload):
    with pytest.raises(ValueError, match="secret-like"):
        validate_factor_payload(
            {**_payload(), **payload},
            as_of=AS_OF,
        )


def test_structured_unknowns_are_exact_bounded_and_public_safe():
    with pytest.raises(TypeError, match="must be a mapping"):
        validate_factor_unknowns(["roe_5y"])
    with pytest.raises(ValueError, match="invalid fields"):
        validate_factor_unknowns([{"component": "quality", "metric": "roe_5y"}])
    with pytest.raises(ValueError, match="component is invalid"):
        validate_factor_unknowns([_unknown(component="other")])
    with pytest.raises(ValueError, match="secret-like"):
        validate_factor_unknowns(
            [_unknown(reason="sk-" + "b" * 20)]
        )
    with pytest.raises(ValueError, match=f"exceeds {MAX_FACTOR_UNKNOWNS}"):
        validate_factor_unknowns(
            [_unknown(metric=f"metric_{index}") for index in range(MAX_FACTOR_UNKNOWNS + 1)]
        )


def test_dataset_hashes_are_bounded_lowercase_sha256_references():
    with pytest.raises(TypeError, match="must be an array"):
        validate_factor_dataset_hashes(HASH_A)
    for invalid in ("A" * 64, "a" * 63, "z" * 64, f" {HASH_A}"):
        with pytest.raises(ValueError, match="lowercase SHA-256"):
            validate_factor_dataset_hashes([invalid])
    with pytest.raises(ValueError, match=f"exceeds {MAX_FACTOR_DATASET_HASHES}"):
        validate_factor_dataset_hashes(
            [f"{index:064x}" for index in range(MAX_FACTOR_DATASET_HASHES + 1)]
        )


def test_factor_payload_enforces_recursive_shape_and_size_limits():
    with pytest.raises(TypeError, match="must be a mapping"):
        validate_factor_payload([], as_of=AS_OF)
    with pytest.raises(ValueError, match="object keys"):
        validate_factor_payload({"x" * (MAX_FACTOR_KEY_CHARS + 1): 1}, as_of=AS_OF)
    with pytest.raises(ValueError, match="object members"):
        validate_factor_payload(
            {str(index): index for index in range(MAX_FACTOR_MAPPING_ITEMS + 1)},
            as_of=AS_OF,
        )
    with pytest.raises(ValueError, match="characters"):
        validate_factor_payload(
            {"note": "x" * (MAX_FACTOR_STRING_CHARS + 1)},
            as_of=AS_OF,
        )

    nested = {"leaf": 1}
    for _ in range(MAX_FACTOR_TREE_DEPTH + 1):
        nested = {"nested": nested}
    with pytest.raises(ValueError, match="nesting depth"):
        validate_factor_payload(nested, as_of=AS_OF)

    oversized = {
        f"field_{index}": "x" * MAX_FACTOR_STRING_CHARS
        for index in range(70)
    }
    with pytest.raises(ValueError, match="UTF-8 bytes"):
        validate_factor_payload(oversized, as_of=AS_OF)
