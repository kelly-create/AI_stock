from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
import json

import pytest

from src.services.research.canonical import CanonicalJSONError, canonical_hash
from src.services.research.repositories import LeaseFence, ResearchSnapshotInput
from src.services.research.snapshot_service import (
    DEBATE_FIELD_DICTIONARY_VERSION,
    DEBATE_SNAPSHOT_VERSION,
    EVIDENCE_FIELD_DICTIONARY_VERSION,
    EVIDENCE_SNAPSHOT_VERSION,
    FrozenResearchSnapshot,
    build_research_snapshot,
    model_route_fingerprint,
    persist_research_snapshot,
    project_model_route,
    project_structured_datasets,
    safe_project_context_pack,
)


AS_OF = datetime(2025, 6, 30, 18, 0, tzinfo=timezone(timedelta(hours=8)))
AVAILABLE_AT = AS_OF - timedelta(minutes=5)


class _SafePack:
    def __init__(self, payload):
        self.payload = payload
        self.called = False

    def to_safe_dict(self):
        self.called = True
        return self.payload


def _route():
    return {
        "backend": "litellm",
        "model": "primary-model",
        "channel": "analysis",
        "deployment": "primary-deployment",
        "temperature": 0.1,
        "reasoning_effort": "medium",
        "max_tokens": 4096,
        "response_format": {"type": "json_object"},
        "base_url": "https://alice:secret@llm.example.com:8443/v1/?api_key=hidden#fragment",
        "api_key": "must-never-affect-route",
        "fallbacks": [
            {
                "provider": "openai-compatible",
                "model": "fallback-a",
                "channel": "analysis",
                "api_base": "https://bob:password@fallback.example.com/v1?token=hidden",
                "api_key": "ignored",
            },
            "fallback-b",
        ],
    }


def _context(*, price=100.0, job_id="job-a", trace_id="trace-a", latency_ms=12):
    return {
        "subject": {"code": "600519", "market": "A"},
        "pack_version": "1.0",
        "blocks": {
            "quote": {
                "status": "available",
                "items": {"close": {"status": "available", "value": price}},
            }
        },
        "job_id": job_id,
        "trace_id": trace_id,
        "created_at": "2025-06-30T17:59:00+08:00",
        "latency_ms": latency_ms,
    }


def _datasets(*, roe=20.0):
    return {
        "fundamental": {
            "dataset": "fundamental",
            "status": "available",
            "data_as_of": "2025-04-30T00:00:00+08:00",
            "available_at": "2025-04-30T08:00:00+08:00",
            "normalized": {"roe": roe},
        }
    }


def _factors(*, value_score=70.0):
    return {
        "as_of": "2025-06-30T15:00:00+08:00",
        "value": {"status": "available", "score": value_score},
        "quality": {"status": "available", "score": 80.0},
    }


def _build(**overrides) -> FrozenResearchSnapshot:
    values = {
        "stock_code": "600519",
        "market": "A",
        "as_of": AS_OF,
        "available_at": AVAILABLE_AT,
        "context_pack": _context(),
        "datasets": _datasets(),
        "factors": _factors(),
        "prompt_version": "personal-research-v1",
        "prompt": {"system": "frozen prompt", "temperature": 0},
        "model_route": _route(),
        "policy_version": "personal-policy-v1",
        "policy": {"value_gate": 65, "risk_max": 45},
        "pack_version": "1.0",
        "factor_snapshot_hash": "a" * 64,
    }
    values.update(overrides)
    return build_research_snapshot(**values)


def _evidence_payload() -> dict:
    return {
        "evidence_engine_version": "research-evidence-v1",
        "claim_policy_version": "research-claims-v1",
        "as_of": "2025-06-30T10:00:00Z",
        "available_at": "2025-06-30T09:55:00Z",
        "status": "available",
        "coverage": 1.0,
        "claims": [{"id": "claim-1", "status": "supported"}],
        "citations": [{"id": "citation-1", "artifact_hash": "d" * 64}],
        "limitations": [],
    }


def _debate_payload() -> dict:
    return {
        "debate_engine_version": "research-debate-v1",
        "output_schema_version": "research-debate-output-v1",
        "prompt_version": "research-debate-prompt-v1",
        "stock_code": "600519",
        "market": "A",
        "as_of": "2025-06-30T10:00:00Z",
        "available_at": "2025-06-30T09:55:00Z",
        "status": "available",
        "evidence_snapshot_hash": "e" * 64,
        "request_hash": "r" * 64,
        "model_route_fingerprint": "m" * 64,
        "bull_turn_hash": "b" * 64,
        "bear_turn_hash": "c" * 64,
        "turns": [
            {
                "stance": "bull",
                "summary": "Bounded upside interpretation.",
                "arguments": [],
                "open_questions": [],
            },
            {
                "stance": "bear",
                "summary": "Bounded downside interpretation.",
                "arguments": [],
                "open_questions": [],
            },
        ],
        "failed_stances": [],
        "limitations": [],
    }


def test_evidence_is_strictly_additive_and_v1_identity_remains_unchanged():
    legacy = _build()
    explicit_none = _build(evidence=None, evidence_snapshot_hash=None)

    assert explicit_none.snapshot_hash == legacy.snapshot_hash
    assert explicit_none.canonical_json == legacy.canonical_json
    assert explicit_none.evidence_snapshot_hash is None
    assert "evidence" not in json.loads(legacy.canonical_json)


@pytest.mark.parametrize(
    "field_name",
    (
        "snapshot_version",
        "field_dictionary_version",
        "factor_engine_version",
        "pack_version",
        "prompt_version",
        "policy_version",
    ),
)
def test_public_snapshot_versions_reject_secret_like_identifiers(
    field_name: str,
) -> None:
    with pytest.raises(ValueError, match="secret-like"):
        _build(**{field_name: "password:supersecret"})


def test_evidence_v2_is_linked_into_payload_hash_and_repository_input():
    evidence_hash = "e" * 64
    frozen = _build(
        evidence=_evidence_payload(),
        evidence_snapshot_hash=evidence_hash,
        snapshot_version=EVIDENCE_SNAPSHOT_VERSION,
        field_dictionary_version=EVIDENCE_FIELD_DICTIONARY_VERSION,
    )

    assert frozen.snapshot_version == EVIDENCE_SNAPSHOT_VERSION
    assert frozen.field_dictionary_version == EVIDENCE_FIELD_DICTIONARY_VERSION
    assert frozen.evidence_snapshot_hash == evidence_hash
    assert json.loads(frozen.canonical_json)["evidence"]["claims"][0]["id"] == "claim-1"
    assert frozen.snapshot_hash != _build().snapshot_hash
    assert frozen.to_repository_input().evidence_snapshot_hash == evidence_hash


def test_evidence_payload_and_hash_must_be_supplied_together():
    with pytest.raises(ValueError, match="both be set"):
        _build(evidence=_evidence_payload())
    with pytest.raises(ValueError, match="both be set"):
        _build(evidence_snapshot_hash="e" * 64)


def test_debate_v3_is_strictly_additive_and_linked_to_evidence():
    evidence_hash = "e" * 64
    evidence_v2 = _build(
        evidence=_evidence_payload(),
        evidence_snapshot_hash=evidence_hash,
        snapshot_version=EVIDENCE_SNAPSHOT_VERSION,
        field_dictionary_version=EVIDENCE_FIELD_DICTIONARY_VERSION,
    )
    explicit_none = _build(
        evidence=_evidence_payload(),
        evidence_snapshot_hash=evidence_hash,
        debate=None,
        debate_snapshot_hash=None,
        snapshot_version=EVIDENCE_SNAPSHOT_VERSION,
        field_dictionary_version=EVIDENCE_FIELD_DICTIONARY_VERSION,
    )
    assert explicit_none.snapshot_hash == evidence_v2.snapshot_hash
    assert explicit_none.canonical_json == evidence_v2.canonical_json

    debate_hash = "f" * 64
    frozen = _build(
        evidence=_evidence_payload(),
        evidence_snapshot_hash=evidence_hash,
        debate=_debate_payload(),
        debate_snapshot_hash=debate_hash,
        snapshot_version=DEBATE_SNAPSHOT_VERSION,
        field_dictionary_version=DEBATE_FIELD_DICTIONARY_VERSION,
    )
    rendered = json.loads(frozen.canonical_json)
    assert frozen.snapshot_version == DEBATE_SNAPSHOT_VERSION
    assert frozen.field_dictionary_version == DEBATE_FIELD_DICTIONARY_VERSION
    assert frozen.debate_snapshot_hash == debate_hash
    assert rendered["debate"]["status"] == "available"
    assert frozen.snapshot_hash != evidence_v2.snapshot_hash
    assert frozen.to_repository_input().debate_snapshot_hash == debate_hash


def test_debate_payload_hash_pair_and_evidence_lineage_are_required():
    with pytest.raises(ValueError, match="both be set"):
        _build(debate=_debate_payload())
    with pytest.raises(ValueError, match="both be set"):
        _build(debate_snapshot_hash="f" * 64)
    with pytest.raises(ValueError, match="requires a frozen evidence"):
        _build(
            debate=_debate_payload(),
            debate_snapshot_hash="f" * 64,
        )
    conflicting = _debate_payload()
    conflicting["evidence_snapshot_hash"] = "d" * 64
    with pytest.raises(ValueError, match="conflicts with frozen evidence"):
        _build(
            evidence=_evidence_payload(),
            evidence_snapshot_hash="e" * 64,
            debate=conflicting,
            debate_snapshot_hash="f" * 64,
        )


def test_snapshot_is_canonical_utf8_stable_and_ignores_execution_metadata():
    first = _build()
    reordered_context = {
        "latency": 999,
        "trace": "different",
        "job": "different",
        "blocks": _context()["blocks"],
        "pack_version": "1.0",
        "subject": {"market": "A", "code": "600519"},
        "created": "2099-01-01T00:00:00Z",
    }
    second = _build(context_pack=reordered_context)

    assert first.snapshot_hash == second.snapshot_hash
    assert first.canonical_json == second.canonical_json
    assert json.loads(first.canonical_json)["context_pack"]["subject"]["code"] == "600519"
    assert "job" not in first.canonical_json
    assert "trace" not in first.canonical_json
    assert "latency" not in first.canonical_json
    first.canonical_json.encode("utf-8").decode("utf-8")


@pytest.mark.parametrize(
    "override",
    [
        {"context_pack": _context(price=101.0)},
        {"datasets": _datasets(roe=21.0)},
        {"factors": _factors(value_score=71.0)},
        {"prompt": {"system": "changed prompt", "temperature": 0}},
        {"prompt_version": "personal-research-v2"},
        {"model_route": {**_route(), "model": "changed-model"}},
        {"policy": {"value_gate": 66, "risk_max": 45}},
        {"policy_version": "personal-policy-v2"},
    ],
)
def test_snapshot_hash_changes_for_data_prompt_route_or_policy(override):
    assert _build(**override).snapshot_hash != _build().snapshot_hash


def test_fallback_order_is_part_of_model_route_fingerprint():
    route = _route()
    reversed_route = {**route, "fallbacks": list(reversed(route["fallbacks"]))}
    assert model_route_fingerprint(route) != model_route_fingerprint(reversed_route)


def test_model_route_projection_keeps_only_contract_fields_and_sanitizes_urls():
    route = _route()
    projected = project_model_route(route)
    rendered = json.dumps(projected, ensure_ascii=False, sort_keys=True)

    assert projected["base_url"]["origin"] == "https://llm.example.com:8443"
    assert projected["base_url"]["path_fingerprint"]
    assert projected["fallbacks"][0]["base_url"]["origin"] == "https://fallback.example.com"
    assert projected["fallbacks"][0]["base_url"]["path_fingerprint"]
    assert "/v1" not in rendered
    assert "alice" not in rendered
    assert "secret" not in rendered
    assert "password" not in rendered
    assert "api_key" not in rendered
    assert "token=" not in rendered
    assert projected["deployment"] == "primary-deployment"
    assert projected["temperature"] == 0.1
    assert projected["reasoning_effort"] == "medium"
    assert projected["max_tokens"] == 4096
    assert projected["response_format"] == {"type": "json_object"}
    changed_secret = {**route, "api_key": "different-secret"}
    assert model_route_fingerprint(changed_secret) == model_route_fingerprint(route)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("deployment", "different-deployment"),
        ("deployments", [{"model": "deployment-a"}]),
        ("fallback_backend", "responses"),
        ("temperature", 0.2),
        ("top_p", 0.8),
        ("top_k", 40),
        ("min_p", 0.05),
        ("seed", 7),
        ("reasoning_effort", "high"),
        ("reasoning", {"effort": "high"}),
        ("thinking", {"type": "enabled", "budget_tokens": 2048}),
        ("extra_body", {"thinking": {"type": "enabled"}}),
        ("max_tokens", 2048),
        ("max_completion_tokens", 3000),
        ("max_output_tokens", 3000),
        ("generation_backend_max_output_bytes", 131072),
        ("response_format", {"type": "text"}),
        ("response_schema", {"type": "object"}),
        ("json_schema", {"name": "decision"}),
        ("tools", [{"type": "web_search"}]),
        ("tool_choice", "required"),
        ("parallel_tool_calls", False),
        ("max_steps", 6),
        ("timeout_seconds", 120.0),
        ("generation_backend_timeout_seconds", 180.0),
        ("frequency_penalty", 0.1),
        ("presence_penalty", 0.1),
        ("repetition_penalty", 1.1),
        ("stop", ["END"]),
        ("verbosity", "low"),
        ("logit_bias", {"42": -2}),
        ("logprobs", True),
        ("top_logprobs", 3),
        ("service_tier", "priority"),
        ("modalities", ["text"]),
        ("n", 2),
    ],
)
def test_every_output_affecting_route_field_changes_fingerprint(field, value):
    route = _route()
    mutated = {**route, field: value}
    assert model_route_fingerprint(mutated) != model_route_fingerprint(route)


def test_nested_route_parameters_and_fallback_parameters_are_fingerprinted_but_secrets_are_not():
    route = _route()
    route["parameters"] = {
        "top_p": 0.9,
        "model": "must-not-overwrite-primary",
        "channel": "must-not-overwrite-channel",
        "apiKey": "nested-secret",
        "authorization": "Bearer nested-secret",
    }
    projected = project_model_route(route)
    assert projected["model"] == "primary-model"
    assert projected["channel"] == "analysis"
    changed = {**route, "parameters": {**route["parameters"], "top_p": 0.8}}
    assert model_route_fingerprint(changed) != model_route_fingerprint(route)

    changed_secret = {
        **route,
        "parameters": {**route["parameters"], "apiKey": "different-secret"},
    }
    assert model_route_fingerprint(changed_secret) == model_route_fingerprint(route)

    fallback_changed = _route()
    fallback_changed["fallbacks"] = [
        {**fallback_changed["fallbacks"][0], "reasoning_effort": "high"},
        fallback_changed["fallbacks"][1],
    ]
    assert model_route_fingerprint(fallback_changed) != model_route_fingerprint(_route())


def test_unknown_non_secret_route_knobs_are_hash_bearing_and_runtime_metadata_is_not():
    route = _route()
    route["customDecodingKnob"] = {"mode": "strict", "strength": 1}
    changed = {
        **route,
        "customDecodingKnob": {"mode": "strict", "strength": 2},
    }
    assert model_route_fingerprint(changed) != model_route_fingerprint(route)

    runtime_changed = {
        **route,
        "job_id": "different-job",
        "trace_id": "different-trace",
        "latency_ms": 9999,
    }
    assert model_route_fingerprint(runtime_changed) == model_route_fingerprint(route)

    with_secret_url = {
        **route,
        "custom_endpoint": "https://user:pass@custom.example.com/path?token=secret",
    }
    projected = project_model_route(with_secret_url)
    rendered = json.dumps(projected, sort_keys=True)
    assert projected["custom_endpoint"]["origin"] == "https://custom.example.com"
    assert projected["custom_endpoint"]["path_fingerprint"]
    assert "/path" not in rendered
    assert "user" not in rendered
    assert "pass" not in rendered
    assert "token=secret" not in rendered

    opaque_route = {**route, "custom_endpoint": "urn:PATH-CREDENTIAL-ROUTE"}
    opaque_projected = project_model_route(opaque_route)
    opaque_rendered = json.dumps(opaque_projected, sort_keys=True)
    assert "PATH-CREDENTIAL-ROUTE" not in opaque_rendered
    assert opaque_projected["custom_endpoint"]["reference_fingerprint"]


@pytest.mark.parametrize(
    ("location", "route_builder"),
    [
        (
            "base",
            lambda secret: {
                **_route(),
                "base_url": f"https://llm.example.com/v1/tenant/{secret}/openai",
            },
        ),
        (
            "fallback",
            lambda secret: {
                **_route(),
                "fallbacks": [
                    {
                        "provider": "openai-compatible",
                        "model": "fallback-a",
                        "api_base": f"https://fallback.example.com/gateway/{secret}/v1",
                    }
                ],
            },
        ),
        (
            "custom",
            lambda secret: {
                **_route(),
                "custom_endpoint": f"https://custom.example.com/signed/{secret}/responses",
            },
        ),
    ],
)
def test_model_route_url_paths_are_fingerprinted_without_persisting_secrets(
    location,
    route_builder,
):
    first_secret = f"PATH-CREDENTIAL-{location}-ALPHA"
    second_secret = f"PATH-CREDENTIAL-{location}-BETA"
    first = route_builder(first_secret)
    second = route_builder(second_secret)

    rendered = json.dumps(project_model_route(first), sort_keys=True)
    assert first_secret not in rendered
    assert "PATH-CREDENTIAL" not in rendered
    assert model_route_fingerprint(first) != model_route_fingerprint(second)


def test_external_news_and_search_keep_only_hash_length_and_sanitized_refs():
    secret_news = "SECRET-NEWS-BODY-DO-NOT-PERSIST"
    secret_title = "SECRET HEADLINE"
    secret_snippet = "SECRET SEARCH SNIPPET"
    context = _context()
    context["blocks"]["news"] = {
        "status": "available",
        "timestamp": "2025-06-30T15:59:00+08:00",
        "items": [
            {
                "title": secret_title,
                "body": secret_news,
                "url": (
                    "https://reader:pass@news.example.com/"
                    "PATH-CREDENTIAL-NEWS/result?id=7&token=secret"
                ),
                "source": "example-news",
                "available_at": "2025-06-30T16:00:00+08:00",
            }
        ],
    }
    datasets = [
        {
            "dataset": "web_search",
            "status": "available",
            "data_as_of": "2025-06-30T15:50:00+08:00",
            "available_at": "2025-06-30T16:00:00+08:00",
            "normalized": {
                "results": [
                    {
                        "title": "Search title",
                        "snippet": secret_snippet,
                        "link": (
                            "https://search.example.com/"
                            "PATH-CREDENTIAL-SEARCH/result?q=secret"
                        ),
                    }
                ]
            },
        }
    ]
    frozen = _build(context_pack=context, datasets=datasets)
    rendered = frozen.canonical_json

    assert secret_news not in rendered
    assert secret_title not in rendered
    assert secret_snippet not in rendered
    assert "reader" not in rendered
    assert "PATH-CREDENTIAL-NEWS" not in rendered
    assert "PATH-CREDENTIAL-SEARCH" not in rendered
    assert "token=secret" not in rendered
    assert "?q=secret" not in rendered
    assert "content_hash" in rendered
    assert "utf8_length" in rendered
    assert '"refs"' in rendered
    assert "2025-06-30T15:59:00+08:00" in rendered

    changed = _context()
    changed["blocks"]["news"] = {
        "status": "available",
        "items": [{"title": secret_title, "body": "different body", "url": "https://news.example.com/a"}],
    }
    assert _build(context_pack=changed, datasets=datasets).snapshot_hash != frozen.snapshot_hash


def test_context_projection_calls_safe_serializer_before_canonicalization():
    pack = _SafePack(_context())
    projected = safe_project_context_pack(pack, as_of=AS_OF)
    assert pack.called is True
    assert projected["subject"]["code"] == "600519"


def test_structured_external_dataset_identifier_triggers_body_projection():
    projected = project_structured_datasets(
        [
            {
                "dataset": "news_items",
                "status": "available",
                "available_at": "2025-06-30T17:00:00+08:00",
                "normalized": {"body": "raw body", "url": "https://example.com/a?token=x"},
            }
        ],
        as_of=AS_OF,
    )
    rendered = json.dumps(projected, ensure_ascii=False)
    assert "raw body" not in rendered
    assert "token=x" not in rendered
    assert "https://example.com/_path_sha256/" in rendered
    assert "content_hash" in rendered


def test_structured_external_dataset_preserves_persisted_lineage_hashes():
    current_hash = "a" * 64
    reused_hash = "b" * 64
    projected = project_structured_datasets(
        {
            "news_search": {
                "dataset": "news_search",
                "status": "partial",
                "available_at": "2025-06-30T17:00:00+08:00",
                "content_hash": current_hash,
                "content_hashes": [current_hash, reused_hash],
                "normalized": {
                    "items": [
                        {
                            "title": "private external body",
                            "url": "https://example.com/a?token=x",
                        }
                    ]
                },
            }
        },
        as_of=AS_OF,
    )

    item = projected["news_search"]
    assert item["content_hash"] == current_hash
    assert item["content_hashes"] == [current_hash, reused_hash]
    rendered = json.dumps(projected, ensure_ascii=False, sort_keys=True)
    assert "private external body" not in rendered
    assert "token=x" not in rendered


def test_safe_projection_drops_sensitive_fields_and_sanitizes_urls_at_any_depth():
    context = _context()
    context["auth"] = {
        "token": "context-token",
        "apiKey": "context-api-key",
        "Authorization": "Bearer context-auth",
        "cookie": "session=context-cookie",
        "password": "context-password",
    }
    context["reference"] = {
        "endpoint": "https://alice:secret@context.example.com/path?q=private#fragment",
        "note": "see https://bob:pass@notes.example.com/doc?token=private now",
        "header_text": "Authorization: Bearer header-secret password=inline-secret",
    }
    datasets = _datasets()
    datasets["fundamental"]["normalized"].update(
        {
            "clientSecret": "dataset-secret",
            "sourceUrl": "https://carol:pass@data.example.com/row?api_key=private",
        }
    )
    factors = _factors()
    factors["provider"] = {
        "accessToken": "factor-token",
        "url": "https://dave:pass@factor.example.com/value?key=private",
    }

    rendered = _build(
        context_pack=context,
        datasets=datasets,
        factors=factors,
    ).canonical_json
    for secret in (
        "context-token",
        "context-api-key",
        "context-auth",
        "context-cookie",
        "context-password",
        "dataset-secret",
        "factor-token",
        "header-secret",
        "inline-secret",
        "alice",
        "bob",
        "carol",
        "dave",
        "private",
    ):
        assert secret not in rendered
    for plaintext_path in ("/path", "/doc", "/row", "/value"):
        assert plaintext_path not in rendered
    for host in (
        "https://context.example.com",
        "https://notes.example.com",
        "https://data.example.com",
        "https://factor.example.com",
    ):
        assert host in rendered
    assert "_path_sha256" in rendered


@pytest.mark.parametrize(
    "secret",
    (
        "sk-abcdefghijklmnopqrstuvwxyz123456",
        "ghp_abcdefghijklmnopqrstuvwxyz123456",
    ),
)
def test_safe_projection_redacts_standalone_token_like_values(secret):
    frozen = _build(
        context_pack={"subject": {"code": "600519", "note": secret}},
        datasets={"daily_basic": {"rows": [{"note": secret}]}},
        factors={"quality": {"note": secret}},
        factor_snapshot_hash="a" * 64,
    )

    rendered = frozen.canonical_json
    assert secret not in rendered
    assert rendered.count("[REDACTED_TOKEN]") == 3


def test_structured_dataset_url_paths_and_opaque_refs_are_fingerprinted():
    secret = "PATH-CREDENTIAL-STRUCTURED"
    projected = project_structured_datasets(
        {
            "endpoint": f"https://api.example.com/signed/{secret}/resource",
            "source_url": f"opaque-{secret}",
            "urn_url": f"urn:{secret}",
            "file_url": f"file:///C:/{secret}",
            "data_url": f"data:text/plain,{secret}",
        },
        as_of=AS_OF,
    )
    rendered = json.dumps(projected, ensure_ascii=False, sort_keys=True)

    assert secret not in rendered
    assert "https://api.example.com/_path_sha256/" in rendered
    assert "REFERENCE_SHA256" in rendered


def test_top_level_and_nested_evidence_times_are_strict():
    with pytest.raises(ValueError, match="available_at cannot be after as_of"):
        _build(available_at=AS_OF + timedelta(seconds=1))
    with pytest.raises(ValueError, match="timezone offset"):
        _build(as_of="2025-06-30T18:00:00")
    future_dataset = _datasets()
    future_dataset["fundamental"]["available_at"] = "2025-07-01T00:00:00+08:00"
    with pytest.raises(ValueError, match="cannot be after snapshot as_of"):
        _build(datasets=future_dataset)
    future_factors = _factors()
    future_factors["as_of"] = "2025-07-01T00:00:00+08:00"
    with pytest.raises(ValueError, match="cannot be after snapshot as_of"):
        _build(factors=future_factors)


@pytest.mark.parametrize(
    ("container", "field"),
    [
        ("context", "published_at"),
        ("context", "publishedAt"),
        ("context", "announced_at"),
        ("context", "announcedAt"),
        ("context", "timestamp"),
        ("context", "providerTimestamp"),
        ("context", "fetchedAt"),
        ("dataset", "announcement_date"),
        ("dataset", "announcementDate"),
    ],
)
def test_future_publication_and_announcement_aliases_fail_closed(container, field):
    context = _context()
    datasets = _datasets()
    if container == "context":
        context["blocks"]["external_news"] = {
            "status": "available",
            "items": [{"title": "hidden", field: "2025-07-01T00:00:00+08:00"}],
        }
    else:
        datasets["fundamental"]["normalized"][field] = "2025-07-01T00:00:00+08:00"
    with pytest.raises(ValueError, match="cannot be after snapshot as_of"):
        _build(context_pack=context, datasets=datasets)


def test_date_only_knowledge_fields_use_conservative_a_share_end_of_day():
    same_day = _datasets()
    same_day["fundamental"]["normalized"]["ann_date"] = "20250630"
    with pytest.raises(ValueError, match="cannot be after snapshot as_of"):
        _build(datasets=same_day)

    prior_day = _datasets()
    prior_day["fundamental"]["normalized"]["announcementDate"] = "2025-06-29"
    assert _build(datasets=prior_day).snapshot_hash


@pytest.mark.parametrize(
    "future_location",
    [
        "metadata",
        "today_value",
        "yesterday_value",
        "date_item",
    ],
)
def test_context_pack_daily_bar_future_dates_fail_closed(future_location):
    context = _context()
    daily_bars = {
        "status": "available",
        "metadata": {"date": "2025-06-30"},
        "items": {
            "today": {
                "status": "available",
                "value": {"date": "2025-06-30", "close": 100.0},
            },
            "yesterday": {
                "status": "available",
                "value": {"date": "2025-06-27", "close": 99.0},
            },
            "date": {
                "status": "available",
                "value": "2025-06-30",
                "metadata": {"date": "2025-06-30"},
            },
        },
    }
    context["blocks"]["daily_bars"] = daily_bars
    if future_location == "metadata":
        daily_bars["metadata"]["date"] = "2025-07-01"
    elif future_location == "today_value":
        daily_bars["items"]["today"]["value"]["date"] = "2025-07-01"
    elif future_location == "yesterday_value":
        daily_bars["items"]["yesterday"]["value"]["date"] = "2025-07-01"
    else:
        daily_bars["items"]["date"]["value"] = "2025-07-01"

    with pytest.raises(ValueError, match="daily_bars.*cannot be after snapshot as_of"):
        _build(context_pack=context)


def test_legacy_daily_bar_context_rejects_future_today_date():
    context = _context()
    context["daily_bars"] = {
        "metadata": {"date": "2025-06-30"},
        "today": {"date": "2025-07-01", "close": 100.0},
        "yesterday": {"date": "2025-06-27", "close": 99.0},
    }

    with pytest.raises(ValueError, match="context_pack.daily_bars.today.date"):
        _build(context_pack=context)


def test_daily_bar_same_day_is_allowed_without_treating_financial_end_date_as_visibility():
    context = _context()
    context["blocks"]["daily_bars"] = {
        "status": "available",
        "metadata": {"date": "2025-06-30"},
        "items": {
            "today": {
                "status": "available",
                "value": {"date": "2025-06-30", "close": 100.0},
            },
        },
    }
    datasets = _datasets()
    datasets["fundamental"]["normalized"]["end_date"] = "2099-12-31"

    assert _build(context_pack=context, datasets=datasets).snapshot_hash


@pytest.mark.parametrize(
    "override",
    [
        {"factors": {"value": float("nan")}},
        {"datasets": {"fundamental": {"roe": float("inf")}}},
        {"prompt": {"temperature": float("nan")}},
        {"policy": {"gate": float("inf")}},
    ],
)
def test_non_finite_values_are_rejected(override):
    with pytest.raises(CanonicalJSONError, match="NaN|infinite"):
        _build(**override)


def test_repository_input_and_optional_persist_use_fence_without_pipeline_dependency():
    frozen = _build()
    repository_input = frozen.to_repository_input()
    assert isinstance(repository_input, ResearchSnapshotInput)
    assert repository_input.canonical_payload == frozen.canonical_payload
    assert repository_input.model_route_fingerprint == frozen.model_route_fingerprint

    expected_hash = canonical_hash(
        {
            "stock_code": repository_input.stock_code,
            "market": repository_input.market,
            "snapshot_version": repository_input.snapshot_version,
            "field_dictionary_version": repository_input.field_dictionary_version,
            "factor_engine_version": repository_input.factor_engine_version,
            "pack_version": repository_input.pack_version,
            "prompt_version": repository_input.prompt_version,
            "policy_version": repository_input.policy_version,
            "model_route_fingerprint": repository_input.model_route_fingerprint,
            "as_of": repository_input.as_of.astimezone(timezone.utc).replace(tzinfo=None),
            "available_at": repository_input.available_at.astimezone(timezone.utc).replace(tzinfo=None),
            "status": repository_input.status,
            "canonical_json": repository_input.canonical_payload,
            "factor_snapshot_hash": repository_input.factor_snapshot_hash,
        }
    )
    assert expected_hash == frozen.snapshot_hash

    marker = object()

    class _Repository:
        def __init__(self):
            self.call = None

        def write_research_snapshot(self, snapshot, *, lease, now=None):
            self.call = (snapshot, lease, now)
            return marker

    repository = _Repository()
    lease = LeaseFence(job_id="job-1", worker_id="worker-1", lease_token="token-1")
    now = AS_OF + timedelta(minutes=1)
    assert persist_research_snapshot(frozen, repository, lease=lease, now=now) is marker
    assert repository.call == (repository_input, lease, now)


def test_frozen_snapshot_is_immutable():
    frozen = _build()
    with pytest.raises(FrozenInstanceError):
        frozen.snapshot_hash = "changed"
    with pytest.raises(TypeError):
        frozen.canonical_payload["factors"]["value"]["score"] = 0
