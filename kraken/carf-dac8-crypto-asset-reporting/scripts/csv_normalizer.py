from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from schemas.carf_common import NormalizedTransaction, normalize_asset, parse_timestamp
from schemas.carf_exchange import canonical_exchange_type

# Additional aliases covering common CASP and tax-export column names.
COLUMN_ALIASES: dict[str, list[str]] = {
    "transaction_id": ["transaction_id", "txid", "id", "order_id", "reference", "refid", "order no.", "trade id"],
    "timestamp": ["timestamp", "date", "datetime", "transaction_date", "time", "created at", "updated time"],
    "transaction_type": ["transaction_type", "type", "category", "side", "operation"],
    "sub_type": ["sub_type", "transaction_sub_type", "carf_code", "tag"],
    "asset_acquired": ["asset_acquired", "asset_in", "buy_asset", "received_asset", "base_asset", "coin"],
    "quantity_acquired": ["quantity_acquired", "amount_in", "buy_amount", "received_amount", "amount", "filled"],
    "asset_disposed": ["asset_disposed", "asset_out", "sell_asset", "sent_asset", "quote_asset", "currency"],
    "quantity_disposed": ["quantity_disposed", "amount_out", "sell_amount", "sent_amount", "executed", "size"],
    "fiat_value": ["fiat_value", "proceeds", "gross_amount", "notional", "total", "subtotal", "amount_usd"],
    "fiat_currency": ["fiat_currency", "currency", "proceeds_currency", "quote", "settlement_currency"],
    "fee": ["fee", "commission", "trading_fee", "fee amount"],
    "fee_currency": ["fee_currency", "commission_currency", "fee_ccy", "fee currency"],
    "jurisdiction": ["jurisdiction", "country", "tax_jurisdiction"],
    "casp_name": ["casp_name", "exchange", "platform", "broker"],
}


def _first(row: dict[str, str], keys: list[str]) -> str:
    lowered = {key.lower(): value for key, value in row.items()}
    for key in keys:
        value = lowered.get(key.lower())
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _to_float(value: str, *, warnings: list[str], field_name: str) -> float:
    raw = (value or "").replace(",", "").replace("$", "").replace("€", "").strip()
    if not raw:
        return 0.0
    try:
        return float(raw)
    except ValueError:
        warnings.append(f"unparseable_numeric:{field_name}:{value}")
        return 0.0


def _normalize_row(
    row: dict[str, str],
    *,
    source_format: str,
    default_type: str,
    casp_name: str,
    jurisdiction: str,
) -> dict[str, Any]:
    warnings: list[str] = []

    def pick(field: str) -> str:
        return _first(row, COLUMN_ALIASES[field])

    transaction_id = pick("transaction_id")
    timestamp_raw = pick("timestamp")
    tx_type = canonical_exchange_type(pick("transaction_type") or default_type)

    tx = NormalizedTransaction(
        transaction_id=transaction_id,
        timestamp=parse_timestamp(timestamp_raw),
        transaction_type=tx_type,
        sub_type=pick("sub_type").upper(),
        asset_acquired=normalize_asset(pick("asset_acquired")),
        quantity_acquired=_to_float(pick("quantity_acquired"), warnings=warnings, field_name="quantity_acquired"),
        asset_disposed=normalize_asset(pick("asset_disposed")),
        quantity_disposed=_to_float(pick("quantity_disposed"), warnings=warnings, field_name="quantity_disposed"),
        fiat_value=_to_float(pick("fiat_value"), warnings=warnings, field_name="fiat_value"),
        fiat_currency=normalize_asset(pick("fiat_currency")),
        fee=_to_float(pick("fee"), warnings=warnings, field_name="fee"),
        fee_currency=normalize_asset(pick("fee_currency")),
        jurisdiction=pick("jurisdiction") or jurisdiction,
        casp_name=pick("casp_name") or casp_name,
        source_format=source_format,
        raw_data={"raw_row": dict(row), "parse_warnings": warnings},
    )
    return tx.as_dict()


def parse_casp_csv(path: str | Path, *, casp_name: str = "unknown_casp") -> tuple[dict[str, str], list[dict[str, Any]]]:
    file_path = Path(path)
    report_id = file_path.stem
    rows: list[dict[str, Any]] = []
    with file_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            normalized = _normalize_row(
                row,
                source_format="CARF_CSV",
                default_type="exchange",
                casp_name=casp_name,
                jurisdiction="UNKNOWN",
            )
            normalized["report_id"] = report_id
            rows.append(normalized)

    metadata = {
        "report_id": report_id,
        "casp_name": casp_name,
        "casp_jurisdiction": "UNKNOWN",
        "reporting_year": "2026",
        "user_tin_hash": "",
        "report_format": "CARF_CSV",
        "source_file": str(file_path),
        "total_records": str(len(rows)),
    }
    return metadata, rows


def parse_user_csv(path: str | Path, *, source: str = "user_csv") -> list[dict[str, Any]]:
    file_path = Path(path)
    rows: list[dict[str, Any]] = []
    with file_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            normalized = _normalize_row(
                row,
                source_format=source,
                default_type="exchange",
                casp_name="",
                jurisdiction="",
            )
            rows.append(normalized)
    return rows
