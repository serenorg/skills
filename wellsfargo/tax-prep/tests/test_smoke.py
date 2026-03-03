"""Smoke tests for the tax preparation builder (no DB required)."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from datetime import date

# Allow importing from scripts/
SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from tax_builder import build_tax_summary, classify_tax_item, render_markdown  # noqa: E402


TAX_MAP = json.loads(
    (Path(__file__).resolve().parent.parent / "config" / "tax_categories.json").read_text()
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


class TestClassifyTaxItem:
    def test_deductible_housing(self) -> None:
        txn = _make_txn(-2000.0, "housing")
        section, key = classify_tax_item(txn, TAX_MAP)
        assert section == "deductible"
        assert key == "housing"

    def test_deductible_utilities(self) -> None:
        txn = _make_txn(-150.0, "utilities")
        section, key = classify_tax_item(txn, TAX_MAP)
        assert section == "deductible"
        assert key == "utilities"

    def test_non_deductible_groceries(self) -> None:
        txn = _make_txn(-300.0, "groceries")
        section, key = classify_tax_item(txn, TAX_MAP)
        assert section == "non_deductible"
        assert key == "groceries"

    def test_non_deductible_dining(self) -> None:
        txn = _make_txn(-75.0, "dining")
        section, key = classify_tax_item(txn, TAX_MAP)
        assert section == "non_deductible"
        assert key == "dining"

    def test_income_payroll(self) -> None:
        txn = _make_txn(5000.0, "payroll")
        section, key = classify_tax_item(txn, TAX_MAP)
        assert section == "income"
        assert key == "payroll"

    def test_income_interest(self) -> None:
        txn = _make_txn(25.0, "interest_income")
        section, key = classify_tax_item(txn, TAX_MAP)
        assert section == "income"
        assert key == "interest_income"

    def test_unknown_positive_goes_to_income(self) -> None:
        txn = _make_txn(200.0, "mystery_category")
        section, key = classify_tax_item(txn, TAX_MAP)
        assert section == "income"
        assert key == "other_income"

    def test_unknown_negative_goes_to_non_deductible(self) -> None:
        txn = _make_txn(-50.0, "mystery_category")
        section, key = classify_tax_item(txn, TAX_MAP)
        assert section == "non_deductible"
        assert key == "uncategorized"

    def test_uncategorized_positive(self) -> None:
        txn = _make_txn(100.0, "uncategorized")
        section, key = classify_tax_item(txn, TAX_MAP)
        assert section == "income"
        assert key == "other_income"

    def test_uncategorized_negative(self) -> None:
        txn = _make_txn(-100.0, "uncategorized")
        section, key = classify_tax_item(txn, TAX_MAP)
        assert section == "non_deductible"
        assert key == "uncategorized"


class TestBuildTaxSummary:
    def test_mixed_transactions(self) -> None:
        transactions = [
            _make_txn(5000.0, "payroll"),
            _make_txn(25.0, "interest_income"),
            _make_txn(-2000.0, "housing"),
            _make_txn(-150.0, "utilities"),
            _make_txn(-300.0, "groceries"),
            _make_txn(-75.0, "dining"),
            _make_txn(-100.0, "insurance"),
        ]
        summary = build_tax_summary(transactions, TAX_MAP)

        assert summary["total_income"] == 5025.0
        assert summary["total_deductible"] == 2250.0
        assert summary["total_non_deductible"] == 375.0

        assert "payroll" in summary["income"]
        assert "interest_income" in summary["income"]
        assert "housing" in summary["deductible"]
        assert "utilities" in summary["deductible"]
        assert "insurance" in summary["deductible"]
        assert "groceries" in summary["non_deductible"]
        assert "dining" in summary["non_deductible"]

    def test_empty_transactions(self) -> None:
        summary = build_tax_summary([], TAX_MAP)
        assert summary["total_income"] == 0.0
        assert summary["total_deductible"] == 0.0
        assert summary["total_non_deductible"] == 0.0
        assert len(summary["income"]) == 0
        assert len(summary["deductible"]) == 0
        assert len(summary["non_deductible"]) == 0

    def test_all_deductible(self) -> None:
        transactions = [
            _make_txn(-1000.0, "housing"),
            _make_txn(-200.0, "utilities"),
            _make_txn(-150.0, "transportation"),
        ]
        summary = build_tax_summary(transactions, TAX_MAP)
        assert summary["total_income"] == 0.0
        assert summary["total_deductible"] == 1350.0
        assert summary["total_non_deductible"] == 0.0

    def test_deductible_items_have_schedule_info(self) -> None:
        transactions = [_make_txn(-500.0, "housing")]
        summary = build_tax_summary(transactions, TAX_MAP)
        housing = summary["deductible"]["housing"]
        assert housing["schedule"] == "C"
        assert housing["line"] == "30"
        assert housing["is_deductible"] is True


class TestRenderMarkdown:
    def test_render_produces_valid_output(self) -> None:
        transactions = [
            _make_txn(5000.0, "payroll"),
            _make_txn(-1500.0, "housing"),
            _make_txn(-200.0, "groceries"),
        ]
        summary = build_tax_summary(transactions, TAX_MAP)
        md = render_markdown(
            summary,
            period_start=date(2025, 1, 1),
            period_end=date(2025, 12, 31),
            run_id="test-run-001",
            txn_count=3,
            tax_year=2025,
        )

        assert "# Wells Fargo Tax Preparation Summary" in md
        assert "2025-01-01" in md
        assert "2025-12-31" in md
        assert "test-run-001" in md
        assert "Tax Year" in md
        assert "Deductible Expenses" in md
        assert "Non-Deductible Expenses" in md
        assert "Total Income" in md
        assert "Total Deductible" in md
        assert "Total Non-Deductible" in md
        assert "Home Office / Rent" in md
        assert "Groceries (Personal)" in md
        assert "W-2 Wages" in md
