from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class ToleranceConfig:
    timestamp_tolerance_seconds: int = 24 * 60 * 60
    quantity_tolerance_pct: float = 0.5
    fiat_tolerance_pct: float = 1.0


def _parse_ts(value: str) -> datetime | None:
    if not value:
        return None
    raw = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _abs_pct_delta(left: float, right: float) -> float:
    base = max(abs(left), abs(right), 1.0)
    return abs(left - right) / base * 100.0


def _classify_discrepancy(
    *,
    qty_delta_pct: float,
    fiat_delta_pct: float,
    ts_delta_seconds: int,
    tolerance: ToleranceConfig,
    fee_delta: float,
) -> str:
    if qty_delta_pct <= tolerance.quantity_tolerance_pct and ts_delta_seconds > tolerance.timestamp_tolerance_seconds:
        return "timezone"
    if qty_delta_pct <= tolerance.quantity_tolerance_pct and fiat_delta_pct <= tolerance.fiat_tolerance_pct:
        return "rounding"
    if abs(fee_delta) > 0 and qty_delta_pct <= tolerance.quantity_tolerance_pct:
        return "fee"
    if qty_delta_pct > tolerance.quantity_tolerance_pct:
        return "quantity"
    return "other"


def _resolution_for(
    *,
    discrepancy_type: str,
    delta_fiat_value: float,
    materiality_threshold_usd: float,
) -> str:
    if discrepancy_type in {"rounding", "timezone", "fee"}:
        return "auto_resolved"
    if abs(delta_fiat_value) >= materiality_threshold_usd or discrepancy_type == "other":
        return "cpa_escalation"
    return "needs_review"


def _candidate_assets(row: dict[str, Any]) -> tuple[str, str]:
    return (
        str(row.get("asset_disposed", "")).upper(),
        str(row.get("asset_acquired", "")).upper(),
    )


def _asset_keys(row: dict[str, Any]) -> set[str]:
    return {token for token in _candidate_assets(row) if token}


def _fiat_home(row: dict[str, Any]) -> float:
    if "fiat_value_home" in row:
        return float(row.get("fiat_value_home", 0.0) or 0.0)
    return float(row.get("fiat_value", 0.0) or 0.0)


def _build_user_index(user_records: list[dict[str, Any]]) -> dict[str, list[int]]:
    index: dict[str, list[int]] = {}
    for idx, row in enumerate(user_records):
        keys = _asset_keys(row)
        if not keys:
            index.setdefault("_EMPTY", []).append(idx)
            continue
        for key in keys:
            index.setdefault(key, []).append(idx)
    return index


def reconcile_transactions(
    *,
    carf_records: list[dict[str, Any]],
    user_records: list[dict[str, Any]],
    tolerance: ToleranceConfig,
    materiality_threshold_usd: float,
) -> dict[str, Any]:
    matches: list[dict[str, Any]] = []
    unmatched_user_indices = set(range(len(user_records)))
    user_index = _build_user_index(user_records)

    for carf in carf_records:
        best_index = None
        best_score = float("inf")
        best_metrics: dict[str, float | int] = {}

        carf_ts = _parse_ts(str(carf.get("timestamp", "")))
        carf_qty = float(carf.get("quantity_disposed", 0.0) or carf.get("quantity_acquired", 0.0) or 0.0)
        carf_value = _fiat_home(carf)
        carf_assets = _asset_keys(carf)

        candidate_indices: set[int] = set()
        if carf_assets:
            for asset in carf_assets:
                candidate_indices.update(user_index.get(asset, []))
        else:
            candidate_indices.update(user_index.get("_EMPTY", []))

        for idx in candidate_indices:
            if idx not in unmatched_user_indices:
                continue
            user = user_records[idx]

            user_ts = _parse_ts(str(user.get("timestamp", "")))
            user_qty = float(user.get("quantity_disposed", 0.0) or user.get("quantity_acquired", 0.0) or 0.0)
            user_value = _fiat_home(user)

            qty_delta_pct = _abs_pct_delta(carf_qty, user_qty)
            value_delta_pct = _abs_pct_delta(carf_value, user_value) if (carf_value or user_value) else 0.0
            ts_delta = abs(int((carf_ts - user_ts).total_seconds())) if (carf_ts and user_ts) else 999_999

            if qty_delta_pct > max(tolerance.quantity_tolerance_pct * 4.0, 5.0):
                continue
            if ts_delta > max(tolerance.timestamp_tolerance_seconds * 2, 172_800):
                continue

            score = qty_delta_pct * 10.0 + value_delta_pct + (ts_delta / 3600.0)
            if score < best_score:
                best_index = idx
                best_score = score
                best_metrics = {
                    "qty_delta_pct": qty_delta_pct,
                    "value_delta_pct": value_delta_pct,
                    "ts_delta": ts_delta,
                    "carf_qty": carf_qty,
                    "user_qty": user_qty,
                    "carf_value": carf_value,
                    "user_value": user_value,
                    "fee_delta": float(carf.get("fee", 0.0) or 0.0) - float(user.get("fee", 0.0) or 0.0),
                }

        if best_index is None:
            delta_value = carf_value
            matches.append(
                {
                    "carf_transaction_id": str(carf.get("transaction_id", "")),
                    "user_transaction_id": "",
                    "match_status": "unmatched_carf",
                    "match_confidence": 0.0,
                    "match_method": "none",
                    "delta_quantity": float(carf.get("quantity_disposed", 0.0) or carf.get("quantity_acquired", 0.0) or 0.0),
                    "delta_fiat_value": delta_value,
                    "delta_timestamp_seconds": 0,
                    "discrepancy_type": "missing",
                    "resolution": _resolution_for(
                        discrepancy_type="missing",
                        delta_fiat_value=delta_value,
                        materiality_threshold_usd=materiality_threshold_usd,
                    ),
                    "resolution_notes": "No corresponding user transaction found.",
                }
            )
            continue

        unmatched_user_indices.discard(best_index)
        user = user_records[best_index]
        qty_delta = float(best_metrics["carf_qty"]) - float(best_metrics["user_qty"])
        value_delta = float(best_metrics["carf_value"]) - float(best_metrics["user_value"])
        ts_delta = int(best_metrics["ts_delta"])

        qty_delta_pct = float(best_metrics["qty_delta_pct"])
        value_delta_pct = float(best_metrics["value_delta_pct"])

        within_tolerance = (
            qty_delta_pct <= tolerance.quantity_tolerance_pct
            and value_delta_pct <= tolerance.fiat_tolerance_pct
            and ts_delta <= tolerance.timestamp_tolerance_seconds
        )

        if within_tolerance:
            discrepancy_type = ""
            match_status = "matched"
            resolution = "auto_resolved"
            notes = "Exact/near-exact within configured tolerances."
        else:
            discrepancy_type = _classify_discrepancy(
                qty_delta_pct=qty_delta_pct,
                fiat_delta_pct=value_delta_pct,
                ts_delta_seconds=ts_delta,
                tolerance=tolerance,
                fee_delta=float(best_metrics["fee_delta"]),
            )
            match_status = "discrepancy"
            resolution = _resolution_for(
                discrepancy_type=discrepancy_type,
                delta_fiat_value=value_delta,
                materiality_threshold_usd=materiality_threshold_usd,
            )
            notes = f"{discrepancy_type} delta detected."

        if bool(carf.get("currency_conversion_missing")) or bool(user.get("currency_conversion_missing")):
            notes += " Missing FX conversion rates; reconciliation used raw fiat values."

        confidence = max(0.0, 1.0 - min(1.0, best_score / 100.0))
        method = "exact" if within_tolerance else "fuzzy"

        matches.append(
            {
                "carf_transaction_id": str(carf.get("transaction_id", "")),
                "user_transaction_id": str(user.get("transaction_id", "")),
                "match_status": match_status,
                "match_confidence": round(confidence, 4),
                "match_method": method,
                "delta_quantity": round(qty_delta, 10),
                "delta_fiat_value": round(value_delta, 4),
                "delta_timestamp_seconds": ts_delta,
                "discrepancy_type": discrepancy_type,
                "resolution": resolution,
                "resolution_notes": notes,
            }
        )

    for idx in sorted(unmatched_user_indices):
        user = user_records[idx]
        delta_value = _fiat_home(user)
        matches.append(
            {
                "carf_transaction_id": "",
                "user_transaction_id": str(user.get("transaction_id", "")),
                "match_status": "unmatched_user",
                "match_confidence": 0.0,
                "match_method": "none",
                "delta_quantity": float(user.get("quantity_disposed", 0.0) or user.get("quantity_acquired", 0.0) or 0.0),
                "delta_fiat_value": delta_value,
                "delta_timestamp_seconds": 0,
                "discrepancy_type": "missing",
                "resolution": _resolution_for(
                    discrepancy_type="missing",
                    delta_fiat_value=delta_value,
                    materiality_threshold_usd=materiality_threshold_usd,
                ),
                "resolution_notes": "User transaction has no corresponding CARF record.",
            }
        )

    summary = {
        "total_carf_records": len(carf_records),
        "total_user_records": len(user_records),
        "matched_count": sum(1 for item in matches if item["match_status"] == "matched"),
        "discrepancy_count": sum(1 for item in matches if item["match_status"] == "discrepancy"),
        "unmatched_carf_count": sum(1 for item in matches if item["match_status"] == "unmatched_carf"),
        "unmatched_user_count": sum(1 for item in matches if item["match_status"] == "unmatched_user"),
        "auto_resolved_count": sum(1 for item in matches if item["resolution"] == "auto_resolved"),
        "needs_review_count": sum(1 for item in matches if item["resolution"] == "needs_review"),
        "cpa_escalation_count": sum(1 for item in matches if item["resolution"] == "cpa_escalation"),
    }

    return {
        "matches": matches,
        "summary": summary,
    }
