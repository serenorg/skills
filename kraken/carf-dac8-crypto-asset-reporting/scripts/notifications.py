from __future__ import annotations

from typing import Any


def build_notifications(*, summary: dict[str, Any], outputs: dict[str, str]) -> list[dict[str, str]]:
    notes: list[dict[str, str]] = []

    cpa_count = int(summary.get("cpa_escalation_count", 0) or 0)
    if cpa_count > 0:
        notes.append(
            {
                "level": "warning",
                "code": "CPA_ESCALATION_REQUIRED",
                "message": f"{cpa_count} items require CPA review before filing.",
                "path": outputs.get("cpa_escalation", ""),
            }
        )

    discrep = int(summary.get("discrepancy_count", 0) or 0)
    if discrep > 0:
        notes.append(
            {
                "level": "info",
                "code": "DISCREPANCIES_FOUND",
                "message": f"{discrep} discrepancy rows found. Review the reconciliation report.",
                "path": outputs.get("report", ""),
            }
        )

    if not notes:
        notes.append(
            {
                "level": "success",
                "code": "RECONCILIATION_CLEAN",
                "message": "No material discrepancies were detected.",
                "path": outputs.get("report", ""),
            }
        )

    return notes
