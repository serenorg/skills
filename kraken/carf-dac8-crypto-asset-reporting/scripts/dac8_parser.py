from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


from carf_parser import parse_carf_xml
from schemas.dac8_extensions import is_emoney_asset, is_high_value_nft


def parse_dac8_xml(path: str | Path) -> tuple[dict[str, str], list[dict[str, object]]]:
    metadata, rows = parse_carf_xml(path)
    metadata["report_format"] = "DAC8_XML"
    metadata["dac8"] = "true"

    for row in rows:
        acquired = str(row.get("asset_acquired", ""))
        disposed = str(row.get("asset_disposed", ""))
        fiat_value = float(row.get("fiat_value", 0.0) or 0.0)
        flags: list[str] = []

        if is_emoney_asset(acquired) or is_emoney_asset(disposed):
            flags.append("dac8_emoney")
        nft_asset = acquired or disposed
        if is_high_value_nft(asset=nft_asset, fiat_value=fiat_value):
            flags.append("dac8_high_value_nft")

        existing = row.get("raw_data")
        if not isinstance(existing, dict):
            existing = {}
        existing["dac8_flags"] = flags
        row["raw_data"] = existing
        row["source_format"] = "DAC8_XML"

    return metadata, rows
