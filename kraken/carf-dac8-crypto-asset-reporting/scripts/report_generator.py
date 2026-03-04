from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _render_template(template: str, mapping: dict[str, str]) -> str:
    out = template
    for key, value in mapping.items():
        out = out.replace("{{" + key + "}}", value)
    return out


def _top_discrepancies(rows: list[dict[str, Any]], limit: int = 10) -> str:
    candidates = [row for row in rows if row.get("match_status") in {"discrepancy", "unmatched_carf", "unmatched_user"}]
    if not candidates:
        return "- none"
    lines: list[str] = []
    for row in candidates[:limit]:
        lines.append(
            "- carf=`{}` user=`{}` status=`{}` type=`{}` resolution=`{}` delta_fiat=`{}`".format(
                row.get("carf_transaction_id", ""),
                row.get("user_transaction_id", ""),
                row.get("match_status", ""),
                row.get("discrepancy_type", ""),
                row.get("resolution", ""),
                row.get("delta_fiat_value", 0),
            )
        )
    return "\n".join(lines)


def generate_reconciliation_outputs(
    *,
    session_id: str,
    summary: dict[str, Any],
    jurisdictions: dict[str, Any],
    matches: list[dict[str, Any]],
    output_dir: str,
    report_template_path: str,
    cpa_template_path: str,
) -> dict[str, str]:
    now = datetime.now(tz=timezone.utc).isoformat()
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    report_template = Path(report_template_path).read_text(encoding="utf-8")
    rendered_report = _render_template(
        report_template,
        {
            "SESSION_ID": session_id,
            "GENERATED_AT": now,
            "SUMMARY": json.dumps(summary, indent=2, sort_keys=True),
            "JURISDICTIONS": json.dumps(jurisdictions, indent=2, sort_keys=True),
            "TOP_DISCREPANCIES": _top_discrepancies(matches),
        },
    )

    report_path = out_dir / f"reconciliation_report_{session_id}.md"
    report_path.write_text(rendered_report, encoding="utf-8")

    summary_path = out_dir / f"reconciliation_summary_{session_id}.json"
    summary_path.write_text(
        json.dumps(
            {
                "session_id": session_id,
                "generated_at": now,
                "summary": summary,
                "jurisdictions": jurisdictions,
                "matches": matches,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    cpa_items = [row for row in matches if row.get("resolution") == "cpa_escalation"]
    paths = {
        "report": str(report_path),
        "summary_json": str(summary_path),
    }

    if cpa_items:
        cpa_template = Path(cpa_template_path).read_text(encoding="utf-8")
        escalation_lines = "\n".join(
            "- carf=`{}` user=`{}` type=`{}` delta_fiat=`{}` notes={}".format(
                row.get("carf_transaction_id", ""),
                row.get("user_transaction_id", ""),
                row.get("discrepancy_type", ""),
                row.get("delta_fiat_value", 0),
                row.get("resolution_notes", ""),
            )
            for row in cpa_items
        )
        rendered = _render_template(
            cpa_template,
            {
                "SESSION_ID": session_id,
                "GENERATED_AT": now,
                "ESCALATION_ITEMS": escalation_lines,
            },
        )
        cpa_path = out_dir / f"cpa_escalation_{session_id}.md"
        cpa_path.write_text(rendered, encoding="utf-8")
        paths["cpa_escalation"] = str(cpa_path)

    return paths
