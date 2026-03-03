"""Pure-logic cash flow statement builder (no DB dependencies)."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any


def load_activity_map(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Activity map not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def classify_activity(
    txn: dict[str, Any],
    activity_map: dict[str, Any],
) -> tuple[str, str, str]:
    """Return (activity, line_item_key, direction) for a transaction.

    activity: 'operating', 'investing', or 'financing'
    direction: 'inflow' or 'outflow'
    """
    category = str(txn.get("category", "uncategorized")).lower().strip()
    amount = float(txn.get("amount", 0))

    for activity in ("operating", "investing", "financing"):
        section = activity_map.get(activity, {})
        if category in section:
            spec = section[category]
            direction = spec.get("direction", "inflow" if amount > 0 else "outflow")
            return activity, category, direction

    # Uncategorized fallback
    default_activity = activity_map.get("uncategorized_default_activity", "operating")
    if amount > 0:
        default_key = activity_map.get("uncategorized_default_inflow_key", "other_operating_inflow")
        return default_activity, default_key, "inflow"
    default_key = activity_map.get("uncategorized_default_outflow_key", "other_operating_outflow")
    return default_activity, default_key, "outflow"


def build_cashflow_statement(
    transactions: list[dict[str, Any]],
    activity_map: dict[str, Any],
) -> dict[str, Any]:
    """Aggregate transactions into a cash flow statement structure."""
    activities: dict[str, dict[str, dict[str, Any]]] = {
        "operating": {},
        "investing": {},
        "financing": {},
    }

    # Pre-populate from map
    for activity_name in ("operating", "investing", "financing"):
        section = activity_map.get(activity_name, {})
        for key, spec in section.items():
            activities[activity_name][key] = {
                "label": spec["label"],
                "direction": spec.get("direction", "outflow"),
                "amount": 0.0,
                "txn_count": 0,
            }

    for txn in transactions:
        activity, item_key, direction = classify_activity(txn, activity_map)
        amount = abs(float(txn.get("amount", 0)))

        bucket = activities[activity]
        if item_key not in bucket:
            bucket[item_key] = {
                "label": item_key.replace("_", " ").title(),
                "direction": direction,
                "amount": 0.0,
                "txn_count": 0,
            }
        bucket[item_key]["amount"] = round(bucket[item_key]["amount"] + amount, 2)
        bucket[item_key]["txn_count"] += 1

    # Remove zero-count items
    for activity_name in ("operating", "investing", "financing"):
        activities[activity_name] = {
            k: v for k, v in activities[activity_name].items() if v["txn_count"] > 0
        }

    # Compute net per activity: inflows minus outflows
    def _net(items: dict[str, dict[str, Any]]) -> float:
        total = 0.0
        for item in items.values():
            if item["direction"] == "inflow":
                total += item["amount"]
            else:
                total -= item["amount"]
        return round(total, 2)

    operating_net = _net(activities["operating"])
    investing_net = _net(activities["investing"])
    financing_net = _net(activities["financing"])
    net_cash_change = round(operating_net + investing_net + financing_net, 2)

    return {
        "operating": activities["operating"],
        "investing": activities["investing"],
        "financing": activities["financing"],
        "operating_net": operating_net,
        "investing_net": investing_net,
        "financing_net": financing_net,
        "net_cash_change": net_cash_change,
    }


def render_markdown(
    statement: dict[str, Any],
    period_start: date,
    period_end: date,
    run_id: str,
    txn_count: int,
) -> str:
    lines: list[str] = []
    lines.append("# Wells Fargo Cash Flow Statement")
    lines.append("")
    lines.append(f"**Period:** {period_start.isoformat()} to {period_end.isoformat()}")
    lines.append(f"**Run ID:** {run_id}")
    lines.append(f"**Transactions analyzed:** {txn_count}")
    lines.append("")

    for activity, title in [
        ("operating", "Operating Activities"),
        ("investing", "Investing Activities"),
        ("financing", "Financing Activities"),
    ]:
        items = statement.get(activity, {})
        net_key = f"{activity}_net"
        lines.append(f"## {title}")
        lines.append("")
        lines.append("| Line Item | Direction | Amount | Txns |")
        lines.append("|-----------|-----------|-------:|-----:|")

        inflows = {k: v for k, v in items.items() if v["direction"] == "inflow"}
        outflows = {k: v for k, v in items.items() if v["direction"] == "outflow"}

        for key, item in sorted(inflows.items(), key=lambda x: -x[1]["amount"]):
            lines.append(f"| {item['label']} | Inflow | ${item['amount']:,.2f} | {item['txn_count']} |")
        for key, item in sorted(outflows.items(), key=lambda x: -x[1]["amount"]):
            lines.append(f"| {item['label']} | Outflow | (${item['amount']:,.2f}) | {item['txn_count']} |")

        net_val = statement[net_key]
        sign = "" if net_val >= 0 else "-"
        display = f"${abs(net_val):,.2f}" if net_val >= 0 else f"(${abs(net_val):,.2f})"
        lines.append(f"| **Net {title}** | | **{display}** | |")
        lines.append("")

    lines.append("## Summary")
    lines.append("")
    lines.append("| | Amount |")
    lines.append("|--|-------:|")
    for activity, label in [
        ("operating_net", "Net Operating"),
        ("investing_net", "Net Investing"),
        ("financing_net", "Net Financing"),
    ]:
        val = statement[activity]
        display = f"${val:,.2f}" if val >= 0 else f"(${abs(val):,.2f})"
        lines.append(f"| {label} | {display} |")

    ncc = statement["net_cash_change"]
    display = f"${ncc:,.2f}" if ncc >= 0 else f"(${abs(ncc):,.2f})"
    lines.append(f"| **Net Cash Change** | **{display}** |")
    lines.append("")

    return "\n".join(lines) + "\n"
