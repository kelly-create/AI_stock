"""Focused service and API tests for immutable personal-research artifacts."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.v1.endpoints import personal_research_artifacts as endpoint
from api.v1.router import router as v1_router
from src.services.personal_research_artifact_query_service import (
    PersonalResearchArtifactContractError,
    PersonalResearchArtifactNotFoundError,
    PersonalResearchArtifactQueryService,
)
from src.services.research.personal_skill_contract import PERSONAL_RESEARCH_SKILL_IDS


HASHES = {str(index): str(index) * 64 for index in range(1, 10)}
NOW = datetime(2026, 8, 10, 8, 0, tzinfo=timezone.utc)


def _skill_row(skill_id: str = "personal-value-quality") -> SimpleNamespace:
    return SimpleNamespace(
        execution_hash=HASHES["1"],
        task_id="task-1",
        stock_code="600519",
        market="cn",
        skill_id=skill_id,
        skill_version="v1",
        contract_hash=HASHES["2"],
        score_field={
            "personal-value-quality": "value_quality_score",
            "personal-trend-timing": "trend_timing_score",
            "personal-catalyst": "catalyst_score",
            "personal-risk": "risk_score",
            "personal-evidence-quality": "evidence_quality_score",
        }[skill_id],
        research_snapshot_hash=HASHES["3"],
        factor_snapshot_hash=HASHES["4"],
        evidence_snapshot_hash=HASHES["5"],
        dataset_snapshot_hashes_json=json.dumps([HASHES["6"]]),
        dataset_lineage_hash=HASHES["7"],
        canonical_input_json=json.dumps({"contract": "input-v1"}),
        input_hash=HASHES["8"],
        result_status="succeeded",
        canonical_output_json=json.dumps({"evidence_refs": ["citation-1"]}),
        output_hash=HASHES["9"],
        score=73.0,
        created_at=NOW,
    )


def _review_row() -> SimpleNamespace:
    return SimpleNamespace(
        review_hash=HASHES["1"],
        task_id="task-1",
        stock_code="600519",
        market="cn",
        debate_snapshot_hash=HASHES["2"],
        evidence_snapshot_hash=HASHES["3"],
        verifier_version="verifier-v1",
        verifier_input_hash=HASHES["4"],
        verifier_output_hash=HASHES["5"],
        verifier_valid=True,
        verifier_fail_closed=False,
        verifier_reason_codes_json="[]",
        verifier_input_json="{}",
        verifier_output_json="{}",
        judge_version="judge-v1",
        judge_policy_hash=HASHES["6"],
        judge_input_hash=HASHES["7"],
        judge_output_hash=HASHES["8"],
        judge_fail_closed=False,
        judge_reason_codes_json="[]",
        verdict="balanced",
        winner=None,
        judge_input_json="{}",
        judge_output_json="{}",
        created_at=NOW,
    )


def _thesis_row() -> SimpleNamespace:
    skill_hashes = {
        "value_quality_execution_hash": HASHES["2"],
        "trend_timing_execution_hash": HASHES["3"],
        "catalyst_execution_hash": HASHES["4"],
        "risk_execution_hash": HASHES["5"],
        "evidence_quality_execution_hash": HASHES["6"],
    }
    scores = {
        "value_quality_score": 70.0,
        "trend_timing_score": 71.0,
        "catalyst_score": 72.0,
        "risk_score": 73.0,
        "evidence_quality_score": 74.0,
    }
    return SimpleNamespace(
        thesis_hash=HASHES["1"],
        thesis_version="personal-research-thesis-v1",
        task_id="task-1",
        stock_code="600519",
        market="cn",
        research_snapshot_hash=HASHES["7"],
        **skill_hashes,
        debate_snapshot_hash=None,
        debate_review_hash=None,
        decision_signal_id=None,
        policy_evaluation_hash=None,
        policy_version=None,
        policy_hash=None,
        portfolio_snapshot_ref=None,
        stance="bullish",
        account_action="open_candidate",
        scores_json=json.dumps(scores),
        catalysts_json=json.dumps(["earnings"]),
        invalidators_json=json.dumps(["margin decline"]),
        unknowns_json=json.dumps(["guidance"]),
        evidence_refs_json=json.dumps(["citation-1"]),
        canonical_content_json=json.dumps({"scores": scores}),
        content_hash=HASHES["8"],
        supersedes_thesis_hash=None,
        created_at=NOW,
    )


class _SkillRepository:
    def __init__(self, rows=None):
        self.rows = list(rows or [])

    def get_by_hash(self, execution_hash):
        del execution_hash
        return self.rows[0] if self.rows else None

    def list_for_task_stock(self, **kwargs):
        self.query = kwargs
        return self.rows


class _ReviewRepository:
    def __init__(self, row=None):
        self.row = row

    def get_by_hash(self, review_hash):
        del review_hash
        return self.row


class _ThesisRepository:
    def __init__(self, row=None):
        self.row = row

    def get_by_hash(self, thesis_hash):
        del thesis_hash
        return self.row

    def get_latest_by_signal_id(self, decision_signal_id):
        self.signal_id = decision_signal_id
        return self.row

    def get_latest_for_task_stock(self, **kwargs):
        self.query = kwargs
        return self.row


def _service(*, skills=None, review=None, thesis=None):
    return PersonalResearchArtifactQueryService(
        skill_repository=_SkillRepository(skills),
        review_repository=_ReviewRepository(review),
        thesis_repository=_ThesisRepository(thesis),
    )


def test_service_lists_five_skills_in_contract_order_with_lineage():
    rows = [_skill_row(skill_id) for skill_id in reversed(PERSONAL_RESEARCH_SKILL_IDS)]
    for index, row in enumerate(rows, start=1):
        row.execution_hash = f"{index:x}" * 64
    result = _service(skills=rows).list_skill_executions(
        task_id="task-1", market="CN", stock_code="600519"
    )

    assert result["contract"] == "personal-research-skill-execution-collection"
    assert result["version"] == "v1"
    assert result["complete"] is True
    assert result["missing_skill_ids"] == []
    assert [item["skill_contract"]["skill_id"] for item in result["executions"]] == list(
        PERSONAL_RESEARCH_SKILL_IDS
    )
    assert result["executions"][0]["lineage"]["research_snapshot_hash"] == HASHES["3"]


def test_service_reports_partial_skill_set_and_rejects_unknown_artifact():
    result = _service(skills=[_skill_row()]).list_skill_executions(
        task_id="task-1", market="cn", stock_code="600519"
    )
    assert result["complete"] is False
    assert set(result["missing_skill_ids"]) == set(PERSONAL_RESEARCH_SKILL_IDS[1:])

    with pytest.raises(PersonalResearchArtifactNotFoundError):
        _service().get_skill_execution(HASHES["1"])


def test_service_exposes_review_and_latest_thesis_lineage():
    service = _service(review=_review_row(), thesis=_thesis_row())
    review = service.get_debate_review(HASHES["1"])
    thesis = service.get_latest_thesis_by_signal_id(42)
    latest = service.get_latest_thesis_for_task_stock(
        task_id="task-1", market="cn", stock_code="600519"
    )

    assert review["contract"] == "personal-research-debate-review"
    assert review["lineage"]["debate_snapshot_hash"] == HASHES["2"]
    assert thesis["lineage"]["skill_execution_hashes"]["personal-risk"] == HASHES["5"]
    assert latest["content_hash"] == HASHES["8"]


def test_service_fails_closed_on_corrupt_persisted_json():
    row = _skill_row()
    row.canonical_output_json = "[]"
    with pytest.raises(PersonalResearchArtifactContractError):
        _service(skills=[row]).get_skill_execution(HASHES["1"])


class _EndpointService:
    def get_skill_execution(self, execution_hash):
        assert execution_hash == HASHES["1"]
        return _service(skills=[_skill_row()]).get_skill_execution(execution_hash)

    def list_skill_executions(self, **kwargs):
        return _service(skills=[_skill_row()]).list_skill_executions(**kwargs)

    def get_debate_review(self, review_hash):
        return _service(review=_review_row()).get_debate_review(review_hash)

    def get_thesis(self, thesis_hash):
        return _service(thesis=_thesis_row()).get_thesis(thesis_hash)

    def get_latest_thesis_by_signal_id(self, decision_signal_id):
        return _service(thesis=_thesis_row()).get_latest_thesis_by_signal_id(
            decision_signal_id
        )

    def get_latest_thesis_for_task_stock(self, **kwargs):
        return _service(thesis=_thesis_row()).get_latest_thesis_for_task_stock(
            **kwargs
        )


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setattr(endpoint, "_service", lambda: _EndpointService())
    app = FastAPI()
    app.include_router(endpoint.router, prefix="/research")
    return TestClient(app)


@pytest.mark.parametrize(
    ("path", "contract"),
    [
        (
            f"/research/personal/artifacts/skills/{HASHES['1']}",
            "personal-research-skill-execution",
        ),
        (
            "/research/personal/artifacts/skills/tasks/task-1/stocks/cn/600519",
            "personal-research-skill-execution-collection",
        ),
        (
            f"/research/personal/artifacts/debate-reviews/{HASHES['1']}",
            "personal-research-debate-review",
        ),
        (
            f"/research/personal/artifacts/theses/{HASHES['1']}",
            "personal-research-thesis",
        ),
        (
            "/research/personal/artifacts/theses/by-signal/42",
            "personal-research-thesis",
        ),
        (
            "/research/personal/artifacts/theses/latest?task_id=task-1&market=cn&stock_code=600519",
            "personal-research-thesis",
        ),
    ],
)
def test_api_returns_explicit_contract_version_and_lineage(client, path, contract):
    response = client.get(path)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["contract"] == contract
    assert body["version"]
    assert body["lineage"]["task_id"] == "task-1"


def test_api_returns_404_for_unknown_artifact(monkeypatch):
    class _MissingService:
        def get_thesis(self, thesis_hash):
            del thesis_hash
            raise PersonalResearchArtifactNotFoundError("missing")

    monkeypatch.setattr(endpoint, "_service", lambda: _MissingService())
    app = FastAPI()
    app.include_router(endpoint.router, prefix="/research")
    response = TestClient(app).get(
        f"/research/personal/artifacts/theses/{HASHES['1']}"
    )
    assert response.status_code == 404
    assert response.json()["detail"]["error"] == "not_found"


def test_api_rejects_malformed_hash_without_calling_service(client):
    response = client.get("/research/personal/artifacts/skills/not-a-hash")
    assert response.status_code == 422


def test_api_declares_same_admin_cookie_security_as_research_reads(client):
    operation = client.get("/openapi.json").json()["paths"][
        "/research/personal/artifacts/theses/{thesis_hash}"
    ]["get"]
    assert operation["security"] == [{"AdminSessionCookie": []}]


def test_v1_router_wires_all_personal_research_artifact_paths():
    included = [
        route
        for route in v1_router.routes
        if getattr(route, "original_router", None) is endpoint.router
    ]
    assert len(included) == 1
    assert included[0].include_context.prefix == "/research"
    paths = {f"/research{route.path}" for route in endpoint.router.routes}
    assert {
        "/research/personal/artifacts/skills/tasks/{task_id}/stocks/{market}/{stock_code}",
        "/research/personal/artifacts/skills/{execution_hash}",
        "/research/personal/artifacts/debate-reviews/{review_hash}",
        "/research/personal/artifacts/theses/by-signal/{decision_signal_id}",
        "/research/personal/artifacts/theses/latest",
        "/research/personal/artifacts/theses/{thesis_hash}",
    }.issubset(paths)
