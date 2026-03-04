from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
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


def _key_asset(row: dict[str, Any]) -> str:
    return str(row.get("asset_disposed") or row.get("asset_acquired") or "").upper()


def _parse_iso(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def merge_bridge_records(
    *,
    primary_records: list[dict[str, Any]],
    bridge_records: list[dict[str, Any]],
    timestamp_tolerance_seconds: int,
    quantity_tolerance_pct: float,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    merged = list(primary_records)
    consumed_primary: set[int] = set()

    asset_index: dict[str, list[int]] = {}
    for idx, row in enumerate(merged):
        asset = _key_asset(row)
        if asset:
            asset_index.setdefault(asset, []).append(idx)

    dual_reported = 0
    for bridge in bridge_records:
        matched = False
        candidates = asset_index.get(_key_asset(bridge), [])

        for idx in candidates:
            if idx in consumed_primary:
                continue
            primary = merged[idx]
            primary_qty = float(primary.get("quantity_disposed", 0.0) or primary.get("quantity_acquired", 0.0) or 0.0)
            bridge_qty = float(bridge.get("quantity_disposed", 0.0) or bridge.get("quantity_acquired", 0.0) or 0.0)

            qty_base = max(abs(primary_qty), abs(bridge_qty), 1.0)
            qty_pct = abs(primary_qty - bridge_qty) / qty_base * 100.0
            if qty_pct > quantity_tolerance_pct:
                continue

            left = _parse_iso(str(primary.get("timestamp", "")))
            right = _parse_iso(str(bridge.get("timestamp", "")))
            if left and right:
                ts_delta = abs((left - right).total_seconds())
                if ts_delta > timestamp_tolerance_seconds:
                    continue

            primary.setdefault("raw_data", {})
            if isinstance(primary["raw_data"], dict):
                primary["raw_data"]["dual_reported"] = True
                primary["raw_data"]["dual_report_sources"] = [
                    primary.get("source_format", "CARF"),
                    bridge.get("source_format", "1099DA"),
                ]
            consumed_primary.add(idx)
            matched = True
            dual_reported += 1
            break

        if not matched:
            merged.append(bridge)
            idx = len(merged) - 1
            asset = _key_asset(bridge)
            if asset:
                asset_index.setdefault(asset, []).append(idx)

    return merged, {
        "bridge_total": len(bridge_records),
        "dual_reported": dual_reported,
        "bridge_added": len(merged) - len(primary_records),
    }
