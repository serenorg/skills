"""Smoke tests for the cash flow statement builder (no DB required)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

# Allow importing from scripts/
SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from cashflow_builder import build_cashflow_statement, classify_activity, render_markdown  # noqa: E402
from datetime import date  # noqa: E402


ACTIVITY_MAP = json.loads(
    (Path(__file__).resolve().parent.parent / "config" / "activity_map.json").read_text()
)


def _make_txn(amount: float, category: str = "uncategorized") -> dict:
    return {
        "row_hash": f"hash_{abs(hash((amount, category)))}",
        "account_masked": "****1234",
        "txn_date": "2025-06-15",
        "description_raw": f"Test txn {category}",
        "amount": amount,
        "currency": "USD",
        "category": category,
        "category_source": "test",
        "confidence": 1.0,
    }


class TestClassifyActivity:
    def test_payroll_is_operating_inflow(self) -> None:
        txn = _make_txn(5000.0, "payroll")
        activity, key, direction = classify_activity(txn, ACTIVITY_MAP)
        assert activity == "operating"
        assert key == "payroll"
        assert direction == "inflow"

    def test_housing_is_operating_outflow(self) -> None:
        txn = _make_txn(-2000.0, "housing")
        activity, key, direction = classify_activity(txn, ACTIVITY_MAP)
        assert activity == "operating"
        assert key == "housing"
        assert direction == "outflow"

    def test_transfers_in_is_financing(self) -> None:
        txn = _make_txn(500.0, "transfers_in")
        activity, key, direction = classify_activity(txn, ACTIVITY_MAP)
        assert activity == "financing"
        assert key == "transfers_in"
        assert direction == "inflow"

    def test_transfers_out_is_financing(self) -> None:
        txn = _make_txn(-300.0, "transfers_out")
        activity, key, direction = classify_activity(txn, ACTIVITY_MAP)
        assert activity == "financing"
        assert key == "transfers_out"
        assert direction == "outflow"

    def test_uncategorized_positive_is_operating_inflow(self) -> None:
        txn = _make_txn(100.0, "uncategorized")
        activity, key, direction = classify_activity(txn, ACTIVITY_MAP)
        assert activity == "operating"
        assert key == "other_operating_inflow"
        assert direction == "inflow"

    def test_uncategorized_negative_is_operating_outflow(self) -> None:
        txn = _make_txn(-50.0, "uncategorized")
        activity, key, direction = classify_activity(txn, ACTIVITY_MAP)
        assert activity == "operating"
        assert key == "other_operating_outflow"
        assert direction == "outflow"

    def test_unknown_category_positive(self) -> None:
        txn = _make_txn(200.0, "mystery_category")
        activity, key, direction = classify_activity(txn, ACTIVITY_MAP)
        assert activity == "operating"
        assert direction == "inflow"

    def test_unknown_category_negative(self) -> None:
        txn = _make_txn(-75.0, "mystery_category")
        activity, key, direction = classify_activity(txn, ACTIVITY_MAP)
        assert activity == "operating"
        assert direction == "outflow"


class TestBuildCashflowStatement:
    def test_basic_statement(self) -> None:
        transactions = [
            _make_txn(5000.0, "payroll"),
            _make_txn(50.0, "interest_income"),
            _make_txn(-2000.0, "housing"),
            _make_txn(-300.0, "groceries"),
            _make_txn(500.0, "transfers_in"),
            _make_txn(-200.0, "transfers_out"),
        ]
        stmt = build_cashflow_statement(transactions, ACTIVITY_MAP)

        # Operating: 5000 + 50 inflows - 2000 - 300 outflows = 2750
        assert stmt["operating_net"] == 2750.0
        # Financing: 500 inflow - 200 outflow = 300
        assert stmt["financing_net"] == 300.0
        assert stmt["investing_net"] == 0.0
        assert stmt["net_cash_change"] == 3050.0

    def test_empty_transactions(self) -> None:
        stmt = build_cashflow_statement([], ACTIVITY_MAP)
        assert stmt["operating_net"] == 0.0
        assert stmt["investing_net"] == 0.0
        assert stmt["financing_net"] == 0.0
        assert stmt["net_cash_change"] == 0.0

    def test_all_operating(self) -> None:
        transactions = [
            _make_txn(3000.0, "payroll"),
            _make_txn(-1000.0, "groceries"),
        ]
        stmt = build_cashflow_statement(transactions, ACTIVITY_MAP)
        assert stmt["operating_net"] == 2000.0
        assert stmt["investing_net"] == 0.0
        assert stmt["financing_net"] == 0.0
        assert stmt["net_cash_change"] == 2000.0

    def test_negative_net_cash(self) -> None:
        transactions = [
            _make_txn(1000.0, "payroll"),
            _make_txn(-3000.0, "housing"),
            _make_txn(-500.0, "transfers_out"),
        ]
        stmt = build_cashflow_statement(transactions, ACTIVITY_MAP)
        assert stmt["operating_net"] == -2000.0
        assert stmt["financing_net"] == -500.0
        assert stmt["net_cash_change"] == -2500.0


class TestRenderMarkdown:
    def test_render_produces_valid_markdown(self) -> None:
        transactions = [
            _make_txn(5000.0, "payroll"),
            _make_txn(-1500.0, "housing"),
            _make_txn(500.0, "transfers_in"),
        ]
        stmt = build_cashflow_statement(transactions, ACTIVITY_MAP)
        md = render_markdown(
            stmt,
            period_start=date(2025, 1, 1),
            period_end=date(2025, 12, 31),
            run_id="test-run-001",
            txn_count=3,
        )

        assert "# Wells Fargo Cash Flow Statement" in md
        assert "2025-01-01" in md
        assert "2025-12-31" in md
        assert "test-run-001" in md
        assert "Operating Activities" in md
        assert "Investing Activities" in md
        assert "Financing Activities" in md
        assert "Net Cash Change" in md
        assert "Salary & Wages Received" in md
        assert "Rent / Mortgage Payments" in md
