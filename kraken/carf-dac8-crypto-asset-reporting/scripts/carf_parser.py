from __future__ import annotations

from pathlib import Path
from typing import Any

from defusedxml import ElementTree as DET

from schemas.carf_common import NormalizedTransaction, normalize_asset, parse_timestamp
from schemas.carf_exchange import canonical_exchange_type


def _local(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _find_text(node: Any, names: list[str]) -> str:
    wanted = {value.lower() for value in names}
    for child in list(node):
        if _local(child.tag).lower() in wanted and child.text:
            return child.text.strip()
    return ""


def _find_direct_child(parent: Any, names: list[str]) -> Any | None:
    wanted = {value.lower() for value in names}
    for child in list(parent):
        if _local(child.tag).lower() in wanted:
            return child
    return None


def _message_spec(root: Any) -> Any:
    node = _find_direct_child(root, ["MessageSpec", "Header", "ReportHeader"])
    return node if node is not None else root


def _metadata_from_root(root: Any, file_path: Path) -> dict[str, str]:
    msg = _message_spec(root)
    report_id = _find_text(msg, ["MessageRefId", "ReportId", "MessageReferenceId"]) or file_path.stem
    casp_name = _find_text(msg, ["CaspName", "ReportingFI", "SendingCompanyName"]) or "unknown_casp"
    casp_j = _find_text(msg, ["CaspJurisdiction", "SendingCompanyIN", "CountryCode"]) or "UNKNOWN"

    year = _find_text(msg, ["ReportingYear", "TaxYear", "Year"])
    if not year:
        meta_ts = parse_timestamp(_find_text(msg, ["Timestamp", "CreatedAt"]))
        guess = meta_ts.year if meta_ts else 2026
        year = str(max(1900, min(2100, guess)))

    user_tin = _find_text(msg, ["UserTINHash", "TinHash", "TINHash"])

    return {
        "report_id": report_id,
        "casp_name": casp_name,
        "casp_jurisdiction": casp_j,
        "reporting_year": year,
        "user_tin_hash": user_tin,
        "report_format": "CARF_XML",
        "source_file": str(file_path),
    }


def parse_carf_xml(path: str | Path) -> tuple[dict[str, str], list[dict[str, object]]]:
    file_path = Path(path)
    tree = DET.parse(file_path)
    root = tree.getroot()

    metadata = _metadata_from_root(root, file_path)

    tx_nodes: list[Any] = []
    for node in root.iter():
        name = _local(node.tag).lower()
        if name in {"transactionreport", "transaction", "cryptotransaction"}:
            tx_nodes.append(node)

    records: list[dict[str, object]] = []
    for idx, node in enumerate(tx_nodes, start=1):
        transaction_id = _find_text(node, ["TransactionId", "TxId", "UniqueTransactionId"]) or f"tx-{idx}"
        timestamp = parse_timestamp(
            _find_text(node, ["Timestamp", "TransactionDate", "DateTime", "TransactionDateTime"])
        )
        tx_type = canonical_exchange_type(_find_text(node, ["TransactionType", "Type", "Category"]))
        sub_type = _find_text(node, ["SubType", "TransactionSubType", "CarfCode"]).upper()

        acquired_asset = normalize_asset(
            _find_text(node, ["AssetAcquired", "AssetIn", "BuyAsset", "ReceivedAsset"])
        )
        disposed_asset = normalize_asset(
            _find_text(node, ["AssetDisposed", "AssetOut", "SellAsset", "SentAsset"])
        )

        quantity_acquired = float(
            _find_text(node, ["QuantityAcquired", "AmountIn", "BuyAmount", "ReceivedAmount"]) or 0.0
        )
        quantity_disposed = float(
            _find_text(node, ["QuantityDisposed", "AmountOut", "SellAmount", "SentAmount"]) or 0.0
        )

        fiat_value = float(_find_text(node, ["FiatValue", "Proceeds", "GrossAmount"]) or 0.0)
        fiat_currency = normalize_asset(_find_text(node, ["FiatCurrency", "Currency", "ProceedsCurrency"]))

        fee = float(_find_text(node, ["Fee", "Commission", "TradingFee"]) or 0.0)
        fee_currency = normalize_asset(_find_text(node, ["FeeCurrency", "CommissionCurrency"])) or fiat_currency

        jurisdiction = (
            _find_text(node, ["Jurisdiction", "TaxJurisdiction", "CountryCode"])
            or metadata["casp_jurisdiction"]
        )

        tx = NormalizedTransaction(
            transaction_id=transaction_id,
            timestamp=timestamp,
            transaction_type=tx_type,
            sub_type=sub_type,
            asset_acquired=acquired_asset,
            quantity_acquired=quantity_acquired,
            asset_disposed=disposed_asset,
            quantity_disposed=quantity_disposed,
            fiat_value=fiat_value,
            fiat_currency=fiat_currency,
            fee=fee,
            fee_currency=fee_currency,
            jurisdiction=str(jurisdiction),
            casp_name=str(metadata["casp_name"]),
            source_format="CARF_XML",
            raw_data={"source_node": _local(node.tag), "report_id": metadata["report_id"]},
        )
        row = tx.as_dict()
        row["report_id"] = metadata["report_id"]
        records.append(row)

    metadata["total_records"] = str(len(records))
    return metadata, records
