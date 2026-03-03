"""Smoke tests for the income statement builder (no DB required)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

# Allow importing from scripts/
SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from statement_builder import build_income_statement, classify_transaction, render_markdown  # noqa: E402
from datetime import date  # noqa: E402


LINE_ITEM_MAP = json.loads(
    (Path(__file__).resolve().parent.parent / "config" / "line_item_map.json").read_text()
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


class TestClassifyTransaction:
    def test_payroll_is_income(self) -> None:
        txn = _make_txn(5000.0, "payroll")
        section, key = classify_transaction(txn, LINE_ITEM_MAP)
        assert section == "income"
        assert key == "payroll"

    def test_housing_is_expense(self) -> None:
        txn = _make_txn(-2000.0, "housing")
        section, key = classify_transaction(txn, LINE_ITEM_MAP)
        assert section == "expenses"
        assert key == "housing"

    def test_uncategorized_positive_goes_to_income(self) -> None:
        txn = _make_txn(100.0, "uncategorized")
        section, key = classify_transaction(txn, LINE_ITEM_MAP)
        assert section == "income"
        assert key == "other_income"

    def test_uncategorized_negative_goes_to_expenses(self) -> None:
        txn = _make_txn(-50.0, "uncategorized")
        section, key = classify_transaction(txn, LINE_ITEM_MAP)
        assert section == "expenses"
        assert key == "other_expense"

    def test_unknown_category_positive(self) -> None:
        txn = _make_txn(200.0, "mystery_category")
        section, key = classify_transaction(txn, LINE_ITEM_MAP)
        assert section == "income"
        assert key == "other_income"

    def test_unknown_category_negative(self) -> None:
        txn = _make_txn(-75.0, "mystery_category")
        section, key = classify_transaction(txn, LINE_ITEM_MAP)
        assert section == "expenses"
        assert key == "other_expense"


class TestBuildIncomeStatement:
    def test_basic_statement(self) -> None:
        transactions = [
            _make_txn(5000.0, "payroll"),
            _make_txn(50.0, "interest_income"),
            _make_txn(-2000.0, "housing"),
            _make_txn(-300.0, "groceries"),
            _make_txn(-100.0, "dining"),
        ]
        stmt = build_income_statement(transactions, LINE_ITEM_MAP)

        assert stmt["total_income"] == 5050.0
        assert stmt["total_expenses"] == 2400.0
        assert stmt["net_income"] == 2650.0
        assert "payroll" in stmt["income"]
        assert "interest_income" in stmt["income"]
        assert "housing" in stmt["expenses"]
        assert "groceries" in stmt["expenses"]
        assert "dining" in stmt["expenses"]

    def test_empty_transactions(self) -> None:
        stmt = build_income_statement([], LINE_ITEM_MAP)
        assert stmt["total_income"] == 0.0
        assert stmt["total_expenses"] == 0.0
        assert stmt["net_income"] == 0.0
        assert len(stmt["income"]) == 0
        assert len(stmt["expenses"]) == 0

    def test_all_income(self) -> None:
        transactions = [
            _make_txn(3000.0, "payroll"),
            _make_txn(100.0, "interest_income"),
        ]
        stmt = build_income_statement(transactions, LINE_ITEM_MAP)
        assert stmt["total_income"] == 3100.0
        assert stmt["total_expenses"] == 0.0
        assert stmt["net_income"] == 3100.0

    def test_negative_net_income(self) -> None:
        transactions = [
            _make_txn(1000.0, "payroll"),
            _make_txn(-3000.0, "housing"),
        ]
        stmt = build_income_statement(transactions, LINE_ITEM_MAP)
        assert stmt["net_income"] == -2000.0


class TestRenderMarkdown:
    def test_render_produces_valid_markdown(self) -> None:
        transactions = [
            _make_txn(5000.0, "payroll"),
            _make_txn(-1500.0, "housing"),
        ]
        stmt = build_income_statement(transactions, LINE_ITEM_MAP)
        md = render_markdown(
            stmt,
            period_start=date(2025, 1, 1),
            period_end=date(2025, 12, 31),
            run_id="test-run-001",
            txn_count=2,
        )

        assert "# Wells Fargo Income Statement" in md
        assert "2025-01-01" in md
        assert "2025-12-31" in md
        assert "test-run-001" in md
        assert "Salary & Wages" in md
        assert "Housing & Rent" in md
        assert "Total Income" in md
        assert "Total Expenses" in md
        assert "Net Income" in md
