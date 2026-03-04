from __future__ import annotations

import csv
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from typing import Any

from schemas.carf_common import NormalizedTransaction, normalize_asset, parse_timestamp
from schemas.carf_exchange import canonical_exchange_type


def _first(row: dict[str, str], keys: list[str]) -> str:
    lowered = {key.lower(): value for key, value in row.items()}
    for key in keys:
        value = lowered.get(key.lower())
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _to_float(value: str) -> float:
    raw = (value or "").replace(",", "").strip()
    if not raw:
        return 0.0
    try:
        return float(raw)
    except ValueError:
        return 0.0


def _normalize_row(
    row: dict[str, str],
    *,
    source_format: str,
    default_type: str,
    casp_name: str,
    jurisdiction: str,
) -> dict[str, Any]:
    transaction_id = _first(row, ["transaction_id", "txid", "id", "order_id", "reference"])
    timestamp_raw = _first(row, ["timestamp", "date", "datetime", "transaction_date"])
    tx_type = canonical_exchange_type(_first(row, ["transaction_type", "type", "category"]) or default_type)

    tx = NormalizedTransaction(
        transaction_id=transaction_id,
        timestamp=parse_timestamp(timestamp_raw),
        transaction_type=tx_type,
        sub_type=_first(row, ["sub_type", "transaction_sub_type", "carf_code"]).upper(),
        asset_acquired=normalize_asset(_first(row, ["asset_acquired", "asset_in", "buy_asset", "received_asset"])),
        quantity_acquired=_to_float(_first(row, ["quantity_acquired", "amount_in", "buy_amount", "received_amount"])),
        asset_disposed=normalize_asset(_first(row, ["asset_disposed", "asset_out", "sell_asset", "sent_asset"])),
        quantity_disposed=_to_float(_first(row, ["quantity_disposed", "amount_out", "sell_amount", "sent_amount"])),
        fiat_value=_to_float(_first(row, ["fiat_value", "proceeds", "gross_amount", "notional"])),
        fiat_currency=normalize_asset(_first(row, ["fiat_currency", "currency", "proceeds_currency"])),
        fee=_to_float(_first(row, ["fee", "commission", "trading_fee"])),
        fee_currency=normalize_asset(_first(row, ["fee_currency", "commission_currency", "fee_ccy"])),
        jurisdiction=_first(row, ["jurisdiction", "country", "tax_jurisdiction"]) or jurisdiction,
        casp_name=_first(row, ["casp_name", "exchange", "platform"]) or casp_name,
        source_format=source_format,
        raw_data={"raw_row": dict(row)},
    )
    return tx.as_dict()


def parse_casp_csv(path: str | Path, *, casp_name: str = "unknown_casp") -> tuple[dict[str, str], list[dict[str, Any]]]:
    file_path = Path(path)
    rows: list[dict[str, Any]] = []
    with file_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(
                _normalize_row(
                    row,
                    source_format="CARF_CSV",
                    default_type="exchange",
                    casp_name=casp_name,
                    jurisdiction="UNKNOWN",
                )
            )

    metadata = {
        "report_id": file_path.stem,
        "casp_name": casp_name,
        "casp_jurisdiction": "UNKNOWN",
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
