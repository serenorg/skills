from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from schemas.carf_common import NormalizedTransaction, normalize_asset, parse_timestamp
from schemas.carf_exchange import canonical_exchange_type


def _local(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _find_text(node: ET.Element, names: list[str]) -> str:
    wanted = {value.lower() for value in names}
    for child in list(node):
        if _local(child.tag).lower() in wanted and child.text:
            return child.text.strip()
    return ""


def _find_anywhere(root: ET.Element, names: list[str]) -> str:
    wanted = {value.lower() for value in names}
    for element in root.iter():
        if _local(element.tag).lower() in wanted and element.text:
            return element.text.strip()
    return ""


def parse_carf_xml(path: str | Path) -> tuple[dict[str, str], list[dict[str, object]]]:
    file_path = Path(path)
    tree = ET.parse(file_path)
    root = tree.getroot()

    metadata = {
        "report_id": _find_anywhere(root, ["MessageRefId", "ReportId", "MessageReferenceId"])
        or file_path.stem,
        "casp_name": _find_anywhere(root, ["CaspName", "ReportingFI", "SendingCompanyName"])
        or "unknown_casp",
        "casp_jurisdiction": _find_anywhere(
            root,
            ["CaspJurisdiction", "Jurisdiction", "SendingCompanyIN", "CountryCode"],
        )
        or "UNKNOWN",
        "report_format": "CARF_XML",
        "source_file": str(file_path),
    }

    tx_nodes: list[ET.Element] = []
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
            raw_data={"source_node": _local(node.tag)},
        )
        records.append(tx.as_dict())

    metadata["total_records"] = str(len(records))
    return metadata, records
