from __future__ import annotations

from pathlib import Path

from carf_parser import parse_carf_xml
from dac8_parser import parse_dac8_xml


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_parse_carf_xml_fixture() -> None:
    metadata, rows = parse_carf_xml(FIXTURES / "sample_carf.xml")
    assert metadata["report_format"] == "CARF_XML"
    assert len(rows) == 3
    assert any(row.get("sub_type") == "CARF401" for row in rows)
    assert rows[0]["transaction_id"] == "carf-tx-001"


def test_parse_dac8_xml_flags_extensions() -> None:
    metadata, rows = parse_dac8_xml(FIXTURES / "sample_dac8.xml")
    assert metadata["report_format"] == "DAC8_XML"
    flags = [row.get("raw_data", {}).get("dac8_flags", []) for row in rows]
    assert any("dac8_emoney" in row_flags for row_flags in flags)
    assert any("dac8_high_value_nft" in row_flags for row_flags in flags)
