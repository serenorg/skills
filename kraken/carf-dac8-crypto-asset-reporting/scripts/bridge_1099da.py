from __future__ import annotations

import csv
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from typing import Any

from schemas.carf_common import NormalizedTransaction, normalize_asset, parse_timestamp


def parse_1099da_csv(path: str | Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            tx = NormalizedTransaction(
                transaction_id=str(row.get("transaction_id") or row.get("id") or "").strip(),
                timestamp=parse_timestamp(str(row.get("date") or row.get("timestamp") or "")),
                transaction_type="exchange",
                sub_type="1099DA",
                asset_acquired=normalize_asset(str(row.get("asset_acquired") or row.get("asset_in") or "")),
                quantity_acquired=float(row.get("quantity_acquired") or row.get("amount_in") or 0.0),
                asset_disposed=normalize_asset(str(row.get("asset_disposed") or row.get("asset_out") or "")),
                quantity_disposed=float(row.get("quantity_disposed") or row.get("amount_out") or 0.0),
                fiat_value=float(row.get("proceeds") or row.get("fiat_value") or 0.0),
                fiat_currency=normalize_asset(str(row.get("currency") or row.get("fiat_currency") or "USD")),
                fee=float(row.get("fee") or 0.0),
                fee_currency=normalize_asset(str(row.get("fee_currency") or row.get("currency") or "USD")),
                jurisdiction="US",
                casp_name=str(row.get("broker") or "1099DA_BROKER"),
                source_format="1099DA_CSV",
                raw_data={"raw_row": dict(row)},
            )
            rows.append(tx.as_dict())
    return rows


def merge_bridge_records(
    *,
    primary_records: list[dict[str, Any]],
    bridge_records: list[dict[str, Any]],
    timestamp_tolerance_seconds: int,
    quantity_tolerance_pct: float,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    merged = list(primary_records)
    dual_reported = 0

    for bridge in bridge_records:
        matched = False
        for primary in merged:
            same_asset = (
                str(primary.get("asset_disposed", "")).upper()
                == str(bridge.get("asset_disposed", "")).upper()
            )
            if not same_asset:
                continue

            primary_qty = float(primary.get("quantity_disposed", 0.0) or 0.0)
            bridge_qty = float(bridge.get("quantity_disposed", 0.0) or 0.0)
            qty_base = max(abs(primary_qty), abs(bridge_qty), 1.0)
            qty_pct = abs(primary_qty - bridge_qty) / qty_base * 100.0
            if qty_pct > quantity_tolerance_pct:
                continue

            left = str(primary.get("timestamp", ""))
            right = str(bridge.get("timestamp", ""))
            if left and right:
                from datetime import datetime

                p = datetime.fromisoformat(left.replace("Z", "+00:00"))
                b = datetime.fromisoformat(right.replace("Z", "+00:00"))
                ts_delta = abs((p - b).total_seconds())
                if ts_delta > timestamp_tolerance_seconds:
                    continue

            primary.setdefault("raw_data", {})
            if isinstance(primary["raw_data"], dict):
                primary["raw_data"]["dual_reported"] = True
                primary["raw_data"]["dual_report_sources"] = [
                    primary.get("source_format", "CARF"),
                    bridge.get("source_format", "1099DA"),
                ]
            matched = True
            dual_reported += 1
            break

        if not matched:
            merged.append(bridge)

    return merged, {
        "bridge_total": len(bridge_records),
        "dual_reported": dual_reported,
        "bridge_added": len(merged) - len(primary_records),
    }
