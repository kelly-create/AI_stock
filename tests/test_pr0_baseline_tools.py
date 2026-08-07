from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from scripts import capture_production_baseline as baseline
from scripts import source_manifest
from scripts.source_manifest import (
    ManifestError,
    build_source_manifest,
    compare_source_roots,
    compare_static_asset_roots,
)


def _write_source_tree(root: Path, *, analyzer_body: str = "VALUE = 1\n") -> None:
    (root / "src").mkdir(parents=True)
    (root / "api").mkdir()
    (root / "templates").mkdir()
    (root / "main.py").write_text("from src.analyzer import VALUE\n", encoding="utf-8")
    (root / "requirements.txt").write_text("fastapi==1.0\n", encoding="utf-8")
    (root / "src" / "analyzer.py").write_text(analyzer_body, encoding="utf-8")
    (root / "api" / "routes.py").write_text("ROUTE = '/health'\n", encoding="utf-8")
    (root / "templates" / "report.j2").write_text("{{ report }}\n", encoding="utf-8")


def test_source_manifest_matches_docker_source_boundary_and_ignores_non_copy_runtime_data(tmp_path: Path) -> None:
    root = tmp_path / "app"
    _write_source_tree(root)
    (root / ".env").write_text("TOKEN=do-not-read\n", encoding="utf-8")
    (root / "data").mkdir()
    (root / "data" / "stock_analysis.db").write_bytes(b"runtime-data")

    manifest = build_source_manifest(root)
    paths = {entry["path"] for entry in manifest["entries"]}
    serialized = json.dumps(manifest)

    assert paths == {
        "api/routes.py",
        "main.py",
        "requirements.txt",
        "src/analyzer.py",
        "templates/report.j2",
    }
    assert manifest["profile"] == "docker-app-source-v2"
    assert "do-not-read" not in serialized
    assert "stock_analysis.db" not in serialized


@pytest.mark.parametrize("relative_name", ["src/credentials.json", "src/private.pem", "secrets.py"])
def test_source_manifest_fails_closed_before_reading_sensitive_copy_input(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    relative_name: str,
) -> None:
    root = tmp_path / "app"
    sensitive_path = root / relative_name
    sensitive_path.parent.mkdir(parents=True, exist_ok=True)
    sensitive_path.write_text("secret-sentinel", encoding="utf-8")

    def fail_if_read(_path: Path) -> tuple[str, int, str]:
        raise AssertionError("sensitive content must not be read")

    monkeypatch.setattr(source_manifest, "_content_fingerprint", fail_if_read)

    with pytest.raises(ManifestError, match="sensitive file detected") as exc_info:
        build_source_manifest(root)

    assert "secret-sentinel" not in str(exc_info.value)
    assert sensitive_path.name not in str(exc_info.value)


def test_source_manifest_comparison_reports_each_drift_class(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    deployed = tmp_path / "deployed"
    _write_source_tree(repo)
    _write_source_tree(deployed, analyzer_body="VALUE = 2\n")
    (repo / "src" / "repo_only.py").write_text("REPO_ONLY = True\n", encoding="utf-8")
    (deployed / "src" / "deployed_only.py").write_text("DEPLOYED_ONLY = True\n", encoding="utf-8")

    comparison = compare_source_roots(repo, deployed)

    assert comparison["matches"] is False
    assert comparison["missing_in_deployed"] == ["src/repo_only.py"]
    assert comparison["unexpected_in_deployed"] == ["src/deployed_only.py"]
    assert [item["path"] for item in comparison["content_mismatches"]] == ["src/analyzer.py"]


def test_source_manifest_normalizes_text_line_endings_but_not_binary_content(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    deployed = tmp_path / "deployed"
    _write_source_tree(repo)
    _write_source_tree(deployed)
    (repo / "src" / "analyzer.py").write_bytes(b"VALUE = 1\r\n")
    (deployed / "src" / "analyzer.py").write_bytes(b"VALUE = 1\n")
    (repo / "src" / "asset.jpg").write_bytes(b"binary\r\ncontent")
    (deployed / "src" / "asset.jpg").write_bytes(b"binary\ncontent")

    comparison = compare_source_roots(repo, deployed)

    assert [item["path"] for item in comparison["content_mismatches"]] == ["src/asset.jpg"]


def test_static_asset_manifest_detects_replaced_built_asset(tmp_path: Path) -> None:
    expected = tmp_path / "expected-static"
    deployed = tmp_path / "deployed-static"
    for root in (expected, deployed):
        (root / "assets").mkdir(parents=True)
        (root / "index.html").write_text("<main>DSA</main>\n", encoding="utf-8")
        (root / "assets" / "app.js").write_text("const version = 1\n", encoding="utf-8")

    assert compare_static_asset_roots(expected, deployed)["matches"] is True

    (deployed / "assets" / "app.js").write_text("const version = 2\n", encoding="utf-8")
    comparison = compare_static_asset_roots(expected, deployed)

    assert comparison["profile"] == "docker-static-assets-v1"
    assert comparison["matches"] is False
    assert [item["path"] for item in comparison["content_mismatches"]] == ["assets/app.js"]


def test_static_asset_manifest_rejects_sensitive_file(tmp_path: Path) -> None:
    expected = tmp_path / "static"
    expected.mkdir()
    (expected / "secrets.json").write_text("secret-sentinel", encoding="utf-8")

    with pytest.raises(ManifestError, match="sensitive file detected") as exc_info:
        source_manifest.build_static_asset_manifest(expected)

    assert "secret-sentinel" not in str(exc_info.value)


def _create_database(path: Path, *, with_foreign_key_violation: bool = False) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE portfolio_accounts (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
            CREATE TABLE portfolio_trades (
                id INTEGER PRIMARY KEY,
                account_id INTEGER NOT NULL REFERENCES portfolio_accounts(id)
            );
            CREATE INDEX ix_portfolio_trades_account_id ON portfolio_trades(account_id);
            INSERT INTO portfolio_accounts(id, name) VALUES (1, 'test');
            INSERT INTO portfolio_trades(id, account_id) VALUES (1, 1);
            """
        )
        if with_foreign_key_violation:
            connection.execute("INSERT INTO portfolio_trades(id, account_id) VALUES (2, 987654321)")
        connection.commit()
    finally:
        connection.close()


def test_sqlite_baseline_is_read_only_and_captures_schema_counts_and_integrity(tmp_path: Path) -> None:
    database_path = tmp_path / "production-copy.db"
    _create_database(database_path)

    snapshot = baseline.inspect_sqlite_database(
        database_path,
        ["portfolio_accounts", "portfolio_trades", "decision_signals"],
    )

    assert snapshot["open_mode"] == "read_only"
    assert snapshot["quick_check"] == {"status": "ok", "issue_count": 0}
    assert snapshot["foreign_key_check"] == {"status": "ok", "violation_count": 0}
    assert snapshot["core_table_counts"] == {
        "portfolio_accounts": {"status": "present", "count": 1},
        "portfolio_trades": {"status": "present", "count": 1},
        "decision_signals": {"status": "absent", "count": None},
    }
    assert snapshot["indexes"]["count"] == 1
    assert snapshot["indexes"]["objects"][0]["name"] == "ix_portfolio_trades_account_id"
    assert len(snapshot["schema"]["sha256"]) == 64

    connection = sqlite3.connect(database_path)
    try:
        assert connection.execute("SELECT COUNT(*) FROM portfolio_accounts").fetchone()[0] == 1
    finally:
        connection.close()


def test_default_core_tables_cover_portfolio_daily_snapshots() -> None:
    assert "portfolio_daily_snapshots" in baseline.DEFAULT_CORE_TABLES


def test_required_core_table_acceptance_is_strict_by_default_and_diagnostic_when_opted_out() -> None:
    snapshot = {
        "core_table_counts": {
            "portfolio_accounts": {"status": "present", "count": 1},
            "portfolio_daily_snapshots": {"status": "absent", "count": None},
        },
        "quick_check": {"status": "ok", "issue_count": 0},
        "foreign_key_check": {"status": "ok", "violation_count": 0},
    }
    required = ["portfolio_accounts", "portfolio_daily_snapshots"]

    strict = baseline.evaluate_core_table_acceptance(snapshot, required, strict=True)
    diagnostic = baseline.evaluate_core_table_acceptance(snapshot, required, strict=False)

    assert strict == {
        "mode": "strict",
        "status": "failed",
        "blocking": True,
        "required_core_tables": required,
        "missing_required_core_tables": ["portfolio_daily_snapshots"],
        "failed_integrity_checks": [],
    }
    assert diagnostic == {
        "mode": "diagnostic",
        "status": "warning",
        "blocking": False,
        "required_core_tables": required,
        "missing_required_core_tables": ["portfolio_daily_snapshots"],
        "failed_integrity_checks": [],
    }


@pytest.mark.parametrize(
    ("quick_check", "foreign_key_check", "expected_failures"),
    [
        (
            {"status": "failed", "issue_count": 1},
            {"status": "ok", "violation_count": 0},
            ["quick_check"],
        ),
        (
            {"status": "ok", "issue_count": 0},
            {"status": "failed", "violation_count": 1},
            ["foreign_key_check"],
        ),
    ],
)
def test_integrity_failures_are_blocking_even_in_missing_table_diagnostic_mode(
    quick_check: dict[str, object],
    foreign_key_check: dict[str, object],
    expected_failures: list[str],
) -> None:
    snapshot = {
        "core_table_counts": {"portfolio_accounts": {"status": "present", "count": 1}},
        "quick_check": quick_check,
        "foreign_key_check": foreign_key_check,
    }

    acceptance = baseline.evaluate_core_table_acceptance(
        snapshot,
        ["portfolio_accounts"],
        strict=False,
    )

    assert acceptance["mode"] == "diagnostic"
    assert acceptance["status"] == "failed"
    assert acceptance["blocking"] is True
    assert acceptance["missing_required_core_tables"] == []
    assert acceptance["failed_integrity_checks"] == expected_failures


def test_healthy_integrity_and_present_required_tables_pass_acceptance() -> None:
    snapshot = {
        "core_table_counts": {"portfolio_accounts": {"status": "present", "count": 1}},
        "quick_check": {"status": "ok", "issue_count": 0},
        "foreign_key_check": {"status": "ok", "violation_count": 0},
    }

    acceptance = baseline.evaluate_core_table_acceptance(
        snapshot,
        ["portfolio_accounts"],
        strict=True,
    )

    assert acceptance["status"] == "passed"
    assert acceptance["blocking"] is False
    assert acceptance["failed_integrity_checks"] == []


def test_production_identity_is_required_unless_diagnostic_is_explicit() -> None:
    database_acceptance = {
        "mode": "strict",
        "status": "passed",
        "blocking": False,
        "required_core_tables": ["portfolio_accounts"],
        "missing_required_core_tables": [],
        "failed_integrity_checks": [],
    }

    strict = baseline.evaluate_identity_acceptance(
        database_acceptance,
        image_metadata=None,
        compose_artifacts=[],
        config_artifacts=[],
        strict=True,
    )
    diagnostic = baseline.evaluate_identity_acceptance(
        database_acceptance,
        image_metadata=None,
        compose_artifacts=[],
        config_artifacts=[],
        strict=False,
    )

    assert strict["status"] == "failed"
    assert strict["blocking"] is True
    assert strict["identity_mode"] == "strict"
    assert strict["missing_identity_artifacts"] == ["image", "compose", "config"]
    assert diagnostic["status"] == "warning"
    assert diagnostic["blocking"] is False
    assert diagnostic["identity_mode"] == "diagnostic"


def test_capture_cli_writes_failed_acceptance_report_before_returning_one(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "baseline.json"
    captured_kwargs: list[dict[str, object]] = []

    def fake_capture_baseline(**kwargs: object) -> dict[str, object]:
        captured_kwargs.append(kwargs)
        return {
            "acceptance": {
                "mode": "strict",
                "status": "failed",
                "blocking": True,
                "required_core_tables": ["custom_required"],
                "missing_required_core_tables": ["custom_required"],
                "failed_integrity_checks": [],
            }
        }

    monkeypatch.setattr(baseline, "capture_baseline", fake_capture_baseline)

    exit_code = baseline.main(
        [
            "--database",
            str(tmp_path / "unused.db"),
            "--core-table",
            "custom_required",
            "--output",
            str(output),
        ]
    )

    assert exit_code == 1
    assert json.loads(output.read_text(encoding="utf-8"))["acceptance"]["status"] == "failed"
    assert captured_kwargs[0]["core_tables"] == ["custom_required"]
    assert captured_kwargs[0]["strict_required_tables"] is True
    assert captured_kwargs[0]["strict_identity"] is True


def test_capture_cli_requires_explicit_diagnostic_for_missing_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_capture_baseline(**kwargs: object) -> dict[str, object]:
        assert kwargs["strict_identity"] is False
        return {
            "acceptance": {
                "mode": "strict",
                "identity_mode": "diagnostic",
                "status": "warning",
                "blocking": False,
                "required_core_tables": [],
                "missing_required_core_tables": [],
                "failed_integrity_checks": [],
                "missing_identity_artifacts": ["image", "compose", "config"],
            }
        }

    monkeypatch.setattr(baseline, "capture_baseline", fake_capture_baseline)

    exit_code = baseline.main(
        [
            "--database",
            str(tmp_path / "unused.db"),
            "--diagnostic",
            "--output",
            str(tmp_path / "diagnostic-identity.json"),
        ]
    )

    assert exit_code == 0


def test_capture_cli_allows_explicit_non_strict_diagnostic_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_capture_baseline(**kwargs: object) -> dict[str, object]:
        assert kwargs["strict_required_tables"] is False
        return {
            "acceptance": {
                "mode": "diagnostic",
                "status": "warning",
                "blocking": False,
                "required_core_tables": ["custom_required"],
                "missing_required_core_tables": ["custom_required"],
                "failed_integrity_checks": [],
            }
        }

    monkeypatch.setattr(baseline, "capture_baseline", fake_capture_baseline)

    exit_code = baseline.main(
        [
            "--database",
            str(tmp_path / "unused.db"),
            "--core-table",
            "custom_required",
            "--allow-missing-core-tables",
            "--output",
            str(tmp_path / "diagnostic.json"),
        ]
    )

    assert exit_code == 0


@pytest.mark.parametrize("failed_check", ["quick_check", "foreign_key_check"])
def test_capture_cli_writes_integrity_failure_and_returns_one_even_in_diagnostic_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failed_check: str,
) -> None:
    output = tmp_path / f"{failed_check}.json"

    def fake_capture_baseline(**kwargs: object) -> dict[str, object]:
        assert kwargs["strict_required_tables"] is False
        return {
            "acceptance": {
                "mode": "diagnostic",
                "status": "failed",
                "blocking": True,
                "required_core_tables": ["portfolio_accounts"],
                "missing_required_core_tables": [],
                "failed_integrity_checks": [failed_check],
            }
        }

    monkeypatch.setattr(baseline, "capture_baseline", fake_capture_baseline)

    exit_code = baseline.main(
        [
            "--database",
            str(tmp_path / "unused.db"),
            "--allow-missing-core-tables",
            "--output",
            str(output),
        ]
    )

    report = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 1
    assert report["acceptance"]["failed_integrity_checks"] == [failed_check]


def test_sqlite_baseline_reports_foreign_key_violation_without_row_contents(tmp_path: Path) -> None:
    database_path = tmp_path / "invalid.db"
    _create_database(database_path, with_foreign_key_violation=True)

    snapshot = baseline.inspect_sqlite_database(database_path, ["portfolio_trades"])

    assert snapshot["foreign_key_check"] == {"status": "failed", "violation_count": 1}
    assert "987654321" not in json.dumps(snapshot)
    acceptance = baseline.evaluate_core_table_acceptance(snapshot, ["portfolio_trades"], strict=False)
    assert acceptance["status"] == "failed"
    assert acceptance["blocking"] is True
    assert acceptance["failed_integrity_checks"] == ["foreign_key_check"]


def test_config_fingerprint_never_returns_values_or_host_directory(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    config_dir = tmp_path / "private-config"
    config_dir.mkdir()
    config_path = config_dir / "runtime.env"
    config_path.write_text("LLM_API_KEY=super-secret-value\n", encoding="utf-8")

    fingerprints = baseline.collect_fingerprints([config_path], repo_root)
    serialized = json.dumps(fingerprints)

    assert fingerprints[0]["name"] == "runtime.env"
    assert len(fingerprints[0]["sha256"]) == 64
    assert "super-secret-value" not in serialized
    assert config_dir.name not in serialized


def test_config_fingerprint_rejects_ambiguous_external_basenames(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    first = tmp_path / "one" / "runtime.env"
    second = tmp_path / "two" / "runtime.env"
    first.parent.mkdir()
    second.parent.mkdir()
    first.write_text("A=1\n", encoding="utf-8")
    second.write_text("A=2\n", encoding="utf-8")

    with pytest.raises(baseline.BaselineError, match="duplicate fingerprint label"):
        baseline.collect_fingerprints([first, second], repo_root)


def test_image_capture_uses_only_selected_non_sensitive_docker_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str, str]] = []

    def fake_docker_field(kind: str, target: str, template: str) -> str:
        calls.append((kind, target, template))
        values = {
            "{{.Config.Image}}": "example/dsa:release",
            "{{.Image}}": "sha256:image-id",
            "{{json .RepoDigests}}": '["example/dsa@sha256:digest"]',
            '{{if .Config.Labels}}{{index .Config.Labels "org.opencontainers.image.revision"}}{{end}}': "abc123",
        }
        return values[template]

    monkeypatch.setattr(baseline, "_docker_field", fake_docker_field)

    metadata = baseline.collect_image_metadata(container="daily-stock-server")

    assert metadata == {
        "reference": "example/dsa:release",
        "image_id": "sha256:image-id",
        "repo_digests": ["example/dsa@sha256:digest"],
        "revision_label": "abc123",
    }
    assert all("Env" not in template for _kind, _target, template in calls)


def _canonical_json_sha256(payload: object) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


GOLDEN_V1_LOCK = {
    "field_dictionary": "032a3c2ccac9d6471a017e84aa294190ff4d29eaa681aeb42797367f71f9d224",
    "input_bundle": "824b10a9115eb10ef0931980b462637545abfecd834897fdacc15541e4a4f76a",
    "cases": {
        "consumer_value_quality_600519": "7e6826269c4ce7987b81793975070d42069018e110585dec33bde7d71b796229",
        "bank_601398": "c08a510be2a81497f59a9ca0b0f685e2a5e715a7df07b02575ff88dc8089da11",
        "growth_volatility_300750": "23271da42b18f7da452932ae3a8e175fb069512ec7e74f558f698972ace886c7",
        "insurance_601318": "857c4b058d45aad1557ff98bc17f0204b943d22cf72c359ba4c58610ddd93754",
        "securities_600030": "a1c4238ed7e92c910100dbc98ca2aae5faa787c6235d68bb8df4cafbf4472d3a",
        "st_stock": "7521ed37f296e5d3c53834332caae24315ac5b04efae39c10f6d17e1761b92f0",
        "suspended_stock": "1e90d0452c2730d5659664bc18081832e8fb12546beddffc673ec09ed06825e5",
        "limit_up_not_buyable": "85d9e4cb2e9427ccb8c0c74681cca8100bdca6074406319ce1069ff5f3e244fc",
        "limit_down_not_sellable": "14ce51217bb4fefc68b9aba61c9417414313acd0a0cb9ab6b92ca7cf91d4abca",
        "missing_data": "495a32fa20f1d8b122cbd2cda264b9c1263bc7da4362cd07e5774090c137cf63",
    },
}


def test_research_golden_fixtures_have_canonical_hashes_and_no_future_data() -> None:
    root = Path(__file__).resolve().parent / "fixtures" / "research_golden"
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    field_dictionary = json.loads((root / manifest["field_dictionary"]["path"]).read_text(encoding="utf-8"))
    input_bundle = json.loads((root / manifest["input_bundle"]["path"]).read_text(encoding="utf-8"))

    assert manifest["manifest_version"] == "research-golden-v1"
    assert manifest["field_dictionary"]["expected_sha256"] == GOLDEN_V1_LOCK["field_dictionary"]
    assert manifest["input_bundle"]["expected_sha256"] == GOLDEN_V1_LOCK["input_bundle"]
    assert _canonical_json_sha256(field_dictionary) == manifest["field_dictionary"]["expected_sha256"]
    assert _canonical_json_sha256(input_bundle) == manifest["input_bundle"]["expected_sha256"]

    cases = {case["case_id"]: case for case in input_bundle["cases"]}
    expected_cases = {case["id"]: case for case in manifest["cases"]}
    assert set(cases) == set(expected_cases) == {
        "consumer_value_quality_600519",
        "bank_601398",
        "growth_volatility_300750",
        "insurance_601318",
        "securities_600030",
        "st_stock",
        "suspended_stock",
        "limit_up_not_buyable",
        "limit_down_not_sellable",
        "missing_data",
    }
    fixed_company_cases = {
        "consumer_value_quality_600519",
        "bank_601398",
        "growth_volatility_300750",
        "insurance_601318",
        "securities_600030",
    }
    assert {cases[case_id]["stock_code"] for case_id in fixed_company_cases} == {
        "600519",
        "601398",
        "300750",
        "601318",
        "600030",
    }

    required_fields = set(field_dictionary["required_case_fields"])
    dataset_statuses = set(field_dictionary["dataset_status_values"])
    case_statuses = set(field_dictionary["fields"]["status"]["values"])
    for case_id, case in cases.items():
        expected = expected_cases[case_id]
        assert expected["expected_sha256"] == GOLDEN_V1_LOCK["cases"][case_id]
        assert required_fields <= set(case)
        assert expected["stock_code"] == case["stock_code"]
        assert _canonical_json_sha256(case) == expected["expected_sha256"]
        assert case["fixture_kind"] in {"recorded", "synthetic"}
        assert case["status"] in case_statuses
        assert case["scenario"]["missing_values_are_zero"] is False

        case_as_of = datetime.fromisoformat(case["as_of"])
        case_available_at = datetime.fromisoformat(case["available_at"])
        assert case_available_at <= case_as_of
        dataset_available_times = []
        for dataset in case["datasets"]:
            dataset_as_of = datetime.fromisoformat(dataset["as_of"])
            dataset_available_at = datetime.fromisoformat(dataset["available_at"])
            assert dataset["status"] in dataset_statuses
            assert dataset_as_of <= case_as_of
            assert dataset_available_at <= case_as_of
            dataset_available_times.append(dataset_available_at)
        assert case_available_at == max(dataset_available_times)


def test_research_golden_edge_execution_semantics_are_fixed() -> None:
    path = Path(__file__).resolve().parent / "fixtures" / "research_golden" / "cases.json"
    cases = {case["case_id"]: case for case in json.loads(path.read_text(encoding="utf-8"))["cases"]}

    assert cases["st_stock"]["scenario"]["risk_flags"] == ["st"]
    assert cases["suspended_stock"]["scenario"]["trading_status"] == "suspended"
    assert cases["suspended_stock"]["scenario"]["buy_executable"] is False
    assert cases["suspended_stock"]["scenario"]["sell_executable"] is False
    assert cases["limit_up_not_buyable"]["scenario"]["limit_state"] == "one_price_limit_up"
    assert cases["limit_up_not_buyable"]["scenario"]["buy_executable"] is False
    assert cases["limit_down_not_sellable"]["scenario"]["limit_state"] == "one_price_limit_down"
    assert cases["limit_down_not_sellable"]["scenario"]["sell_executable"] is False
    missing_fundamental = next(
        dataset for dataset in cases["missing_data"]["datasets"] if dataset["dataset"] == "fundamental"
    )
    assert missing_fundamental["status"] == "fetch_failed"
    assert missing_fundamental["value"] is None
