from __future__ import annotations

from typing import Any

DEADLINES = {
    "EU": "2027-09-30",
    "US": "2026-01-31",
    "UK": "2027-09-30",
    "CA": "2027-09-30",
    "SG": "2027-09-30",
    "JP": "2027-09-30",
    "KR": "2027-09-30",
    "AU": "2027-09-30",
}


def _normalize_jurisdiction(value: str) -> str:
    token = (value or "").strip().upper()
    if not token:
        return "UNKNOWN"
    if token in {"FR", "DE", "IT", "ES", "NL", "BE", "AT", "PT", "SE", "PL", "IE", "FI", "DK", "CZ", "RO", "HU", "GR", "BG", "HR", "SK", "SI", "LT", "LV", "EE", "LU", "CY", "MT"}:
        return "EU"
    return token


def detect_jurisdictions(
    *,
    report_metadatas: list[dict[str, Any]],
    normalized_records: list[dict[str, Any]],
) -> dict[str, Any]:
    casp_jurisdictions: set[str] = set()
    user_jurisdictions: set[str] = set()

    for metadata in report_metadatas:
        casp_j = _normalize_jurisdiction(str(metadata.get("casp_jurisdiction", "")))
        if casp_j:
            casp_jurisdictions.add(casp_j)

    for row in normalized_records:
        raw_j = str(row.get("jurisdiction", "")).strip()
        if raw_j:
            user_jurisdictions.add(_normalize_jurisdiction(raw_j))
        raw_data = row.get("raw_data")
        if isinstance(raw_data, dict):
            for key in ("user_residency", "tax_residency", "residency_jurisdiction"):
                value = str(raw_data.get(key, "")).strip()
                if value:
                    user_jurisdictions.add(_normalize_jurisdiction(value))

    deadlines = {j: DEADLINES.get(j, "Check local authority guidance") for j in casp_jurisdictions | user_jurisdictions}

    dual_reporting_flag = len(casp_jurisdictions) > 1 or bool(casp_jurisdictions & user_jurisdictions)

    return {
        "casp_jurisdictions": sorted(casp_jurisdictions),
        "user_jurisdictions": sorted(user_jurisdictions),
        "deadlines": deadlines,
        "dual_reporting_flag": dual_reporting_flag,
    }
