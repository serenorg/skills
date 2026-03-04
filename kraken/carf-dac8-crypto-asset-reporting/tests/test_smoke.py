from __future__ import annotations

import json
from pathlib import Path

from agent import run_once


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures"


def _prepare_runtime_tree(tmp_path: Path) -> Path:
    (tmp_path / "templates").mkdir(parents=True, exist_ok=True)
    (tmp_path / "state").mkdir(parents=True, exist_ok=True)
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "templates" / "reconciliation_report.md").write_text(
        (ROOT / "templates" / "reconciliation_report.md").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (tmp_path / "templates" / "cpa_escalation.md").write_text(
        (ROOT / "templates" / "cpa_escalation.md").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    return tmp_path


def test_run_once_with_bridge_mode(tmp_path: Path, monkeypatch) -> None:
    runtime = _prepare_runtime_tree(tmp_path)
    config = json.loads((ROOT / "config.example.json").read_text(encoding="utf-8"))
    config_path = runtime / "config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")

    monkeypatch.chdir(runtime)
    monkeypatch.setattr("agent.ensure_seren_api_key", lambda cfg: "sb_local_test")

    result = run_once(
        config_path=str(config_path),
        carf_reports=[str(FIXTURES / "sample_carf.xml")],
        user_records=[str(FIXTURES / "sample_transactions.csv")],
        output_dir=str(runtime / "reports"),
        bridge_1099da_path=str(FIXTURES / "sample_1099da.csv"),
        accept_risk_disclaimer=True,
    )

    assert result["status"] == "ok"
    assert result["summary"]["total_carf_records"] >= 3
    assert result["summary"]["bridge"]["bridge_total"] == 1
    assert Path(result["outputs"]["report"]).exists()
    assert Path(result["outputs"]["summary_json"]).exists()
