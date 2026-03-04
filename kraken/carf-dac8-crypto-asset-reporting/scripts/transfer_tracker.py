from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys
from typing import Any

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from schemas.carf_transfer import is_transfer_type


def _parse_ts(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def reconcile_transfers(
    *,
    carf_records: list[dict[str, Any]],
    user_records: list[dict[str, Any]],
    timestamp_tolerance_seconds: int,
    quantity_tolerance_pct: float,
) -> dict[str, Any]:
    carf_transfers = [row for row in carf_records if is_transfer_type(str(row.get("transaction_type", "")))]
    user_transfers = [row for row in user_records if is_transfer_type(str(row.get("transaction_type", "")))]

    matched = 0
    unmatched: list[str] = []
    consumed: set[int] = set()

    for carf in carf_transfers:
        found = False
        c_asset = str(carf.get("asset_disposed") or carf.get("asset_acquired") or "").upper()
        c_qty = float(carf.get("quantity_disposed", 0.0) or carf.get("quantity_acquired", 0.0) or 0.0)
        c_ts = _parse_ts(str(carf.get("timestamp", "")))

        for idx, user in enumerate(user_transfers):
            if idx in consumed:
                continue
            u_asset = str(user.get("asset_disposed") or user.get("asset_acquired") or "").upper()
            if c_asset and u_asset and c_asset != u_asset:
                continue
            u_qty = float(user.get("quantity_disposed", 0.0) or user.get("quantity_acquired", 0.0) or 0.0)
            base = max(abs(c_qty), abs(u_qty), 1.0)
            qty_delta_pct = abs(c_qty - u_qty) / base * 100.0
            if qty_delta_pct > quantity_tolerance_pct:
                continue

            u_ts = _parse_ts(str(user.get("timestamp", "")))
            if c_ts and u_ts:
                if abs((c_ts - u_ts).total_seconds()) > timestamp_tolerance_seconds:
                    continue

            consumed.add(idx)
            matched += 1
            found = True
            break

        if not found:
            unmatched.append(str(carf.get("transaction_id", "")))

    return {
        "carf_transfer_count": len(carf_transfers),
        "user_transfer_count": len(user_transfers),
        "matched_transfer_count": matched,
        "unmatched_transfer_ids": unmatched,
    }
