"""Pure-logic income statement builder (no DB dependencies)."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any


def load_line_item_map(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Line item map not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def classify_transaction(
    txn: dict[str, Any],
    line_item_map: dict[str, Any],
) -> tuple[str, str]:
    """Return (section, line_item_key) for a transaction."""
    category = str(txn.get("category", "uncategorized")).lower().strip()
    amount = float(txn.get("amount", 0))

    for key in line_item_map.get("income", {}):
        if category == key:
            return "income", key

    for key in line_item_map.get("expenses", {}):
        if category == key:
            return "expenses", key

    if category == "uncategorized":
        if amount > 0:
            return "income", "other_income"
        default_section = line_item_map.get("uncategorized_default_section", "expenses")
        default_item = line_item_map.get("uncategorized_default_line_item", "other_expense")
        return default_section, default_item

    if amount > 0:
        return "income", "other_income"
    return "expenses", "other_expense"


def build_income_statement(
    transactions: list[dict[str, Any]],
    line_item_map: dict[str, Any],
) -> dict[str, Any]:
    """Aggregate transactions into an income statement structure."""
    income_items: dict[str, dict[str, Any]] = {}
    expense_items: dict[str, dict[str, Any]] = {}

    for key, spec in line_item_map.get("income", {}).items():
        income_items[key] = {"label": spec["label"], "amount": 0.0, "txn_count": 0}
    for key, spec in line_item_map.get("expenses", {}).items():
        expense_items[key] = {"label": spec["label"], "amount": 0.0, "txn_count": 0}

    for txn in transactions:
        section, item_key = classify_transaction(txn, line_item_map)
        amount = float(txn.get("amount", 0))

        if section == "income":
            if item_key not in income_items:
                income_items[item_key] = {"label": item_key.replace("_", " ").title(), "amount": 0.0, "txn_count": 0}
            income_items[item_key]["amount"] = round(income_items[item_key]["amount"] + amount, 2)
            income_items[item_key]["txn_count"] += 1
        else:
            if item_key not in expense_items:
                expense_items[item_key] = {"label": item_key.replace("_", " ").title(), "amount": 0.0, "txn_count": 0}
            expense_items[item_key]["amount"] = round(expense_items[item_key]["amount"] + abs(amount), 2)
            expense_items[item_key]["txn_count"] += 1

    income_items = {k: v for k, v in income_items.items() if v["txn_count"] > 0}
    expense_items = {k: v for k, v in expense_items.items() if v["txn_count"] > 0}

    total_income = round(sum(v["amount"] for v in income_items.values()), 2)
    total_expenses = round(sum(v["amount"] for v in expense_items.values()), 2)
    net_income = round(total_income - total_expenses, 2)

    return {
        "income": income_items,
        "expenses": expense_items,
        "total_income": total_income,
        "total_expenses": total_expenses,
        "net_income": net_income,
    }


def render_markdown(
    statement: dict[str, Any],
    period_start: date,
    period_end: date,
    run_id: str,
    txn_count: int,
) -> str:
    lines: list[str] = []
    lines.append("# Wells Fargo Income Statement")
    lines.append("")
    lines.append(f"**Period:** {period_start.isoformat()} to {period_end.isoformat()}")
    lines.append(f"**Run ID:** {run_id}")
    lines.append(f"**Transactions analyzed:** {txn_count}")
    lines.append("")

    lines.append("## Income")
    lines.append("")
    lines.append("| Line Item | Amount | Txns |")
    lines.append("|-----------|-------:|-----:|")
    for key, item in sorted(statement["income"].items(), key=lambda x: -x[1]["amount"]):
        lines.append(f"| {item['label']} | ${item['amount']:,.2f} | {item['txn_count']} |")
    lines.append(f"| **Total Income** | **${statement['total_income']:,.2f}** | |")
    lines.append("")

    lines.append("## Expenses")
    lines.append("")
    lines.append("| Line Item | Amount | Txns |")
    lines.append("|-----------|-------:|-----:|")
    for key, item in sorted(statement["expenses"].items(), key=lambda x: -x[1]["amount"]):
        lines.append(f"| {item['label']} | ${item['amount']:,.2f} | {item['txn_count']} |")
    lines.append(f"| **Total Expenses** | **${statement['total_expenses']:,.2f}** | |")
    lines.append("")

    lines.append("## Summary")
    lines.append("")
    lines.append("| | Amount |")
    lines.append("|--|-------:|")
    lines.append(f"| Total Income | ${statement['total_income']:,.2f} |")
    lines.append(f"| Total Expenses | (${statement['total_expenses']:,.2f}) |")
    lines.append(f"| **Net Income** | **${statement['net_income']:,.2f}** |")
    lines.append("")

    return "\n".join(lines) + "\n"
