from __future__ import annotations

from jurisdiction_detector import detect_jurisdictions


def test_detect_jurisdictions_and_deadlines() -> None:
    metadata = [
        {
            "casp_jurisdiction": "FR",
        },
        {
            "casp_jurisdiction": "US",
        },
    ]
    rows = [
        {
            "jurisdiction": "DE",
            "raw_data": {"tax_residency": "US"},
        }
    ]

    detected = detect_jurisdictions(report_metadatas=metadata, normalized_records=rows)
    assert "EU" in detected["casp_jurisdictions"]
    assert "US" in detected["casp_jurisdictions"]
    assert detected["dual_reporting_flag"] is False
    assert "EU" in detected["deadlines"]


def test_detect_dual_reporting_per_transaction() -> None:
    metadata = [
        {"casp_jurisdiction": "FR"},
        {"casp_jurisdiction": "US"},
    ]
    rows = [
        {
            "transaction_id": "tx-1",
            "jurisdiction": "FR",
            "source_format": "DAC8_XML",
            "raw_data": {},
        },
        {
            "transaction_id": "tx-1",
            "jurisdiction": "US",
            "source_format": "CARF_XML",
            "raw_data": {},
        },
    ]

    detected = detect_jurisdictions(report_metadatas=metadata, normalized_records=rows)
    assert detected["dual_reporting_flag"] is True
    assert detected["dual_reporting_transactions"] == ["tx-1"]
