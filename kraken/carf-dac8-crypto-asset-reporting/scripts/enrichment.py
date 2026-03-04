from __future__ import annotations

from collections import deque
from datetime import datetime
from typing import Any

CARF_TREATMENT = {
    "CARF401": "income_fmv_at_receipt",
    "CARF402": "loan_collateral_review_required",
    "CARF403": "potentially_non_taxable_wrap_unwrap",
    "CARF404": "forced_disposition_capital_event",
    "1099DA": "us_broker_reported_disposition",
}


def _parse_ts(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def enrich_tax_treatments(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        subtype = str(item.get("sub_type", "") or "").upper()
        treatment = CARF_TREATMENT.get(subtype)
        if not treatment:
            tx_type = str(item.get("transaction_type", "") or "")
            if "transfer" in tx_type:
                treatment = "non_disposition_transfer"
            else:
                treatment = "disposition_or_exchange"
        item["tax_treatment"] = treatment
        out.append(item)
    return out


def resolve_cost_basis(rows: list[dict[str, Any]], method: str = "fifo") -> list[dict[str, Any]]:
    if method.lower() not in {"fifo", "lifo", "specific"}:
        method = "fifo"

    sorted_rows = sorted(rows, key=lambda row: str(row.get("timestamp", "")))
    lots: dict[str, deque[dict[str, float | str]]] = {}
    output: list[dict[str, Any]] = []

    for row in sorted_rows:
        item = dict(row)
        ts = _parse_ts(str(item.get("timestamp", "")))

        acquired_asset = str(item.get("asset_acquired", "") or "").upper()
        acquired_qty = float(item.get("quantity_acquired", 0.0) or 0.0)
        disposed_asset = str(item.get("asset_disposed", "") or "").upper()
        disposed_qty = float(item.get("quantity_disposed", 0.0) or 0.0)

        fiat_home = float(item.get("fiat_value_home", item.get("fiat_value", 0.0)) or 0.0)

        if acquired_asset and acquired_qty > 0 and fiat_home > 0:
            unit_cost = fiat_home / max(acquired_qty, 1e-12)
            lots.setdefault(acquired_asset, deque()).append(
                {
                    "qty": acquired_qty,
                    "unit_cost": unit_cost,
                    "timestamp": str(item.get("timestamp", "")),
                }
            )

        if disposed_asset and disposed_qty > 0:
            available = lots.setdefault(disposed_asset, deque())
            qty_left = disposed_qty
            consumed_cost = 0.0
            oldest_ts: datetime | None = None

            while qty_left > 1e-12 and available:
                lot = available[-1] if method.lower() == "lifo" else available[0]
                lot_qty = float(lot["qty"])
                take = min(lot_qty, qty_left)
                consumed_cost += take * float(lot["unit_cost"])
                qty_left -= take
                lot["qty"] = lot_qty - take
                lot_ts = _parse_ts(str(lot.get("timestamp", "")))
                if lot_ts and (oldest_ts is None or lot_ts < oldest_ts):
                    oldest_ts = lot_ts
                if float(lot["qty"]) <= 1e-12:
                    if method.lower() == "lifo":
                        available.pop()
                    else:
                        available.popleft()

            proceeds = fiat_home
            gain_loss = proceeds - consumed_cost
            item["cost_basis_home"] = round(consumed_cost, 8)
            item["gain_loss_home"] = round(gain_loss, 8)

            if ts and oldest_ts:
                item["holding_period_days"] = int((ts - oldest_ts).days)
            else:
                item["holding_period_days"] = None

            if qty_left > 1e-12:
                raw_data = dict(item.get("raw_data") or {})
                raw_data["cost_basis_warning"] = "insufficient_lot_inventory"
                item["raw_data"] = raw_data

        output.append(item)

    return output
