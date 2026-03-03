"""Pure-logic tax preparation builder (no DB dependencies)."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any


def load_tax_categories(path: Path) -> dict[str, Any]:
    """Load the tax categories map from a JSON file."""
    if not path.exists():
        raise FileNotFoundError(f"Tax categories map not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def classify_tax_item(
    txn: dict[str, Any],
    tax_map: dict[str, Any],
) -> tuple[str, str]:
    """Return (section, category_key) for a transaction.

    Sections: 'income', 'deductible', 'non_deductible'.
    Falls back to 'non_deductible' / 'uncategorized' for unknown categories.
    """
    category = str(txn.get("category", "uncategorized")).lower().strip()
    amount = float(txn.get("amount", 0))

    # Check income categories first
    for key in tax_map.get("income", {}):
        if category == key:
            return "income", key

    # Check deductible categories
    for key in tax_map.get("deductible", {}):
        if category == key:
            return "deductible", key

    # Check non-deductible categories
    for key in tax_map.get("non_deductible", {}):
        if category == key:
            return "non_deductible", key

    # Fallback: positive amounts -> income, negative -> non_deductible
    if category == "uncategorized" or category not in _all_known_keys(tax_map):
        if amount > 0:
            return "income", "other_income"
        return "non_deductible", "uncategorized"

    return "non_deductible", "uncategorized"


def _all_known_keys(tax_map: dict[str, Any]) -> set[str]:
    """Collect all known category keys across all sections."""
    keys: set[str] = set()
    for section in ("income", "deductible", "non_deductible"):
        keys.update(tax_map.get(section, {}).keys())
    return keys


def build_tax_summary(
    transactions: list[dict[str, Any]],
    tax_map: dict[str, Any],
) -> dict[str, Any]:
    """Aggregate transactions into a tax preparation summary."""
    income_items: dict[str, dict[str, Any]] = {}
    deductible_items: dict[str, dict[str, Any]] = {}
    non_deductible_items: dict[str, dict[str, Any]] = {}

    # Pre-populate from the tax map so labels are available
    for key, spec in tax_map.get("income", {}).items():
        income_items[key] = {
            "label": spec.get("label", key),
            "form": spec.get("form", ""),
            "amount": 0.0,
            "txn_count": 0,
        }
    for key, spec in tax_map.get("deductible", {}).items():
        deductible_items[key] = {
            "label": spec.get("label", key),
            "schedule": spec.get("schedule", ""),
            "line": spec.get("line", ""),
            "is_deductible": True,
            "amount": 0.0,
            "txn_count": 0,
        }
    for key, spec in tax_map.get("non_deductible", {}).items():
        non_deductible_items[key] = {
            "label": spec.get("label", key),
            "is_deductible": False,
            "amount": 0.0,
            "txn_count": 0,
        }

    for txn in transactions:
        section, item_key = classify_tax_item(txn, tax_map)
        amount = float(txn.get("amount", 0))

        if section == "income":
            if item_key not in income_items:
                income_items[item_key] = {
                    "label": item_key.replace("_", " ").title(),
                    "form": "",
                    "amount": 0.0,
                    "txn_count": 0,
                }
            income_items[item_key]["amount"] = round(
                income_items[item_key]["amount"] + amount, 2,
            )
            income_items[item_key]["txn_count"] += 1

        elif section == "deductible":
            if item_key not in deductible_items:
                deductible_items[item_key] = {
                    "label": item_key.replace("_", " ").title(),
                    "schedule": "",
                    "line": "",
                    "is_deductible": True,
                    "amount": 0.0,
                    "txn_count": 0,
                }
            deductible_items[item_key]["amount"] = round(
                deductible_items[item_key]["amount"] + abs(amount), 2,
            )
            deductible_items[item_key]["txn_count"] += 1

        else:  # non_deductible
            if item_key not in non_deductible_items:
                non_deductible_items[item_key] = {
                    "label": item_key.replace("_", " ").title(),
                    "is_deductible": False,
                    "amount": 0.0,
                    "txn_count": 0,
                }
            non_deductible_items[item_key]["amount"] = round(
                non_deductible_items[item_key]["amount"] + abs(amount), 2,
            )
            non_deductible_items[item_key]["txn_count"] += 1

    # Remove zero-count entries
    income_items = {k: v for k, v in income_items.items() if v["txn_count"] > 0}
    deductible_items = {k: v for k, v in deductible_items.items() if v["txn_count"] > 0}
    non_deductible_items = {k: v for k, v in non_deductible_items.items() if v["txn_count"] > 0}

    total_income = round(sum(v["amount"] for v in income_items.values()), 2)
    total_deductible = round(sum(v["amount"] for v in deductible_items.values()), 2)
    total_non_deductible = round(sum(v["amount"] for v in non_deductible_items.values()), 2)

    return {
        "income": income_items,
        "deductible": deductible_items,
        "non_deductible": non_deductible_items,
        "total_income": total_income,
        "total_deductible": total_deductible,
        "total_non_deductible": total_non_deductible,
    }


def render_markdown(
    summary: dict[str, Any],
    period_start: date,
    period_end: date,
    run_id: str,
    txn_count: int,
    tax_year: int | None = None,
) -> str:
    """Render a tax preparation summary as a Markdown report."""
    lines: list[str] = []
    lines.append("# Wells Fargo Tax Preparation Summary")
    lines.append("")
    if tax_year is not None:
        lines.append(f"**Tax Year:** {tax_year}")
    lines.append(f"**Period:** {period_start.isoformat()} to {period_end.isoformat()}")
    lines.append(f"**Run ID:** {run_id}")
    lines.append(f"**Transactions analyzed:** {txn_count}")
    lines.append("")

    # Income section
    lines.append("## Income")
    lines.append("")
    lines.append("| Category | Form | Amount | Txns |")
    lines.append("|----------|------|-------:|-----:|")
    for key, item in sorted(summary["income"].items(), key=lambda x: -x[1]["amount"]):
        form = item.get("form", "")
        lines.append(f"| {item['label']} | {form} | ${item['amount']:,.2f} | {item['txn_count']} |")
    lines.append(f"| **Total Income** | | **${summary['total_income']:,.2f}** | |")
    lines.append("")

    # Deductible expenses section
    lines.append("## Deductible Expenses")
    lines.append("")
    lines.append("| Category | Schedule | Line | Amount | Txns |")
    lines.append("|----------|----------|------|-------:|-----:|")
    for key, item in sorted(summary["deductible"].items(), key=lambda x: -x[1]["amount"]):
        schedule = item.get("schedule", "")
        line = item.get("line", "")
        lines.append(f"| {item['label']} | {schedule} | {line} | ${item['amount']:,.2f} | {item['txn_count']} |")
    lines.append(f"| **Total Deductible** | | | **${summary['total_deductible']:,.2f}** | |")
    lines.append("")

    # Non-deductible expenses section
    lines.append("## Non-Deductible Expenses")
    lines.append("")
    lines.append("| Category | Amount | Txns |")
    lines.append("|----------|-------:|-----:|")
    for key, item in sorted(summary["non_deductible"].items(), key=lambda x: -x[1]["amount"]):
        lines.append(f"| {item['label']} | ${item['amount']:,.2f} | {item['txn_count']} |")
    lines.append(f"| **Total Non-Deductible** | **${summary['total_non_deductible']:,.2f}** | |")
    lines.append("")

    # Summary section
    lines.append("## Summary")
    lines.append("")
    lines.append("| | Amount |")
    lines.append("|--|-------:|")
    lines.append(f"| Total Income | ${summary['total_income']:,.2f} |")
    lines.append(f"| Total Deductible | (${summary['total_deductible']:,.2f}) |")
    lines.append(f"| Total Non-Deductible | (${summary['total_non_deductible']:,.2f}) |")
    lines.append("")
    lines.append("*This summary is an estimate only and is not a substitute for professional tax advice.*")
    lines.append("")

    return "\n".join(lines) + "\n"
