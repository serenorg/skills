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
    assert detected["dual_reporting_flag"] is True
    assert "EU" in detected["deadlines"]
