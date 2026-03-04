from __future__ import annotations

from datetime import datetime
from typing import Any

from schemas.carf_transfer import is_transfer_type


def _parse_ts(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _asset(row: dict[str, Any]) -> str:
    return str(row.get("asset_disposed") or row.get("asset_acquired") or "").upper()


def _qty(row: dict[str, Any]) -> float:
    return float(row.get("quantity_disposed", 0.0) or row.get("quantity_acquired", 0.0) or 0.0)


def reconcile_transfers(
    *,
    carf_records: list[dict[str, Any]],
    user_records: list[dict[str, Any]],
    timestamp_tolerance_seconds: int,
    quantity_tolerance_pct: float,
) -> dict[str, Any]:
    carf_transfers = [row for row in carf_records if is_transfer_type(str(row.get("transaction_type", "")))]
    user_transfers = [row for row in user_records if is_transfer_type(str(row.get("transaction_type", "")))]
    user_non_transfer = [row for row in user_records if not is_transfer_type(str(row.get("transaction_type", "")))]

    matched = 0
    unmatched: list[str] = []
    consumed: set[int] = set()
    misclassified_as_disposition: list[str] = []

    for carf in carf_transfers:
        found = False
        c_asset = _asset(carf)
        c_qty = _qty(carf)
        c_ts = _parse_ts(str(carf.get("timestamp", "")))

        for idx, user in enumerate(user_transfers):
            if idx in consumed:
                continue
            if c_asset and _asset(user) and c_asset != _asset(user):
                continue

            u_qty = _qty(user)
            base = max(abs(c_qty), abs(u_qty), 1.0)
            qty_delta_pct = abs(c_qty - u_qty) / base * 100.0
            if qty_delta_pct > quantity_tolerance_pct:
                continue

            u_ts = _parse_ts(str(user.get("timestamp", "")))
            if c_ts and u_ts and abs((c_ts - u_ts).total_seconds()) > timestamp_tolerance_seconds:
                continue

            consumed.add(idx)
            matched += 1
            found = True
            break

        if not found:
            unmatched.append(str(carf.get("transaction_id", "")))
            for user in user_non_transfer:
                if c_asset and _asset(user) and c_asset != _asset(user):
                    continue
                base = max(abs(c_qty), abs(_qty(user)), 1.0)
                qty_delta_pct = abs(c_qty - _qty(user)) / base * 100.0
                if qty_delta_pct > quantity_tolerance_pct:
                    continue
                u_ts = _parse_ts(str(user.get("timestamp", "")))
                if c_ts and u_ts and abs((c_ts - u_ts).total_seconds()) > timestamp_tolerance_seconds:
                    continue
                misclassified_as_disposition.append(str(carf.get("transaction_id", "")))
                break

    # Heuristic wash-sale signal: same-asset disposition and reacquisition within 30 days.
    wash_sale_candidates: list[str] = []
    exchange_events = [row for row in (carf_records + user_records) if not is_transfer_type(str(row.get("transaction_type", "")))]
    for txid in unmatched:
        row = next((item for item in carf_transfers if str(item.get("transaction_id", "")) == txid), None)
        if row is None:
            continue
        ref_asset = _asset(row)
        ref_ts = _parse_ts(str(row.get("timestamp", "")))
        if not ref_asset or not ref_ts:
            continue

        saw_disposition = False
        saw_reacquire = False
        for event in exchange_events:
            if _asset(event) != ref_asset:
                continue
            event_ts = _parse_ts(str(event.get("timestamp", "")))
            if not event_ts:
                continue
            days = abs((event_ts - ref_ts).days)
            if days > 30:
                continue
            if float(event.get("quantity_disposed", 0.0) or 0.0) > 0:
                saw_disposition = True
            if float(event.get("quantity_acquired", 0.0) or 0.0) > 0:
                saw_reacquire = True
            if saw_disposition and saw_reacquire:
                wash_sale_candidates.append(txid)
                break

    return {
        "carf_transfer_count": len(carf_transfers),
        "user_transfer_count": len(user_transfers),
        "matched_transfer_count": matched,
        "unmatched_transfer_ids": unmatched,
        "potential_wash_sale_ids": sorted(set(wash_sale_candidates)),
        "potential_transfer_as_disposition_ids": sorted(set(misclassified_as_disposition)),
    }
