#!/usr/bin/env python3
"""Wells Fargo Tax Preparation Generator.

Reads categorized transaction data from SerenDB (populated by bank-statement-processing)
and generates a tax preparation summary mapping transactions to IRS tax line items.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import psycopg

from tax_builder import (
    build_tax_summary,
    classify_tax_item,
    load_tax_categories,
    render_markdown,
)

SCRIPT_DIR = Path(__file__).resolve().parent


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def dump_json(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


def append_jsonl(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, sort_keys=True, default=str) + "\n")


def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

class RunLogger:
    def __init__(self, log_path: Path) -> None:
        self.log_path = log_path

    def emit(self, step: str, message: str, **data: Any) -> None:
        payload = {"ts": utc_now_iso(), "step": step, "message": message, "data": data}
        append_jsonl(self.log_path, payload)
        suffix = f" | {json.dumps(data, sort_keys=True, default=str)}" if data else ""
        print(f"[{payload['ts']}] {step}: {message}{suffix}")


# ---------------------------------------------------------------------------
# SerenDB resolution (mirrors bank-statement-processing logic)
# ---------------------------------------------------------------------------

def _run_seren_json(seren_bin: str, args: list[str]) -> tuple[int, Any, str]:
    cmd = [seren_bin, *args, "-o", "json"]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    payload: Any = None
    if result.stdout.strip():
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError:
            pass
    return result.returncode, payload, result.stderr.strip()


def _extract_database_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("databases", "data", "items", "results"):
            if isinstance(payload.get(key), list):
                return payload[key]
    return []


def _parse_dotenv_value(env_path: Path, key: str) -> str:
    if not env_path.exists():
        return ""
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith(f"{key}="):
            return line[len(f"{key}="):].strip().strip("\"'")
    return ""


def resolve_serendb_database_url(
    config: dict[str, Any],
    logger: RunLogger,
) -> tuple[str, str]:
    serendb_cfg = config.get("serendb", {})
    env_key = str(serendb_cfg.get("database_url_env", "WF_SERENDB_URL")).strip() or "WF_SERENDB_URL"

    from_env = os.getenv(env_key, "").strip()
    if from_env:
        return from_env, f"env:{env_key}"

    if not bool(serendb_cfg.get("auto_resolve_via_seren_cli", True)):
        raise RuntimeError(
            f"SerenDB is enabled but {env_key} is empty and auto-resolve is disabled."
        )

    seren_bin = shutil.which("seren")
    if not seren_bin:
        raise RuntimeError(
            f"SerenDB is enabled but {env_key} is empty and `seren` CLI was not found in PATH."
        )

    with tempfile.TemporaryDirectory(prefix="wf-tax-env-") as temp_dir:
        env_path = Path(temp_dir) / ".env"
        base_cmd = [
            seren_bin,
            "env",
            "init",
            "--env",
            str(env_path),
            "--key",
            env_key,
            "--yes",
            "-o",
            "json",
        ]
        if bool(serendb_cfg.get("pooled_connection", True)):
            base_cmd.append("--pooled")

        # Build candidates from database catalog
        rc, payload, _ = _run_seren_json(seren_bin, ["list-all-databases"])
        rows = _extract_database_rows(payload) if rc == 0 else []

        desired_project = str(serendb_cfg.get("project_name", "")).strip().lower()
        desired_database = str(serendb_cfg.get("database_name", "serendb")).strip().lower()

        candidates: list[tuple[str, str, str]] = []
        seen: set[tuple[str, str]] = set()

        # Explicit project_id + branch_id
        explicit_pid = str(serendb_cfg.get("project_id", "")).strip()
        explicit_bid = str(serendb_cfg.get("branch_id", "")).strip()
        if explicit_pid and explicit_bid:
            candidates.append((explicit_pid, explicit_bid, "explicit"))
            seen.add((explicit_pid, explicit_bid))

        # Rank from catalog
        for row in rows:
            pid = row.get("project_id", "").strip()
            bid = row.get("branch_id", "").strip()
            if not pid or not bid:
                continue
            key = (pid, bid)
            if key in seen:
                continue
            rp = row.get("project_name", "").strip().lower()
            rd = row.get("database_name", "").strip().lower()
            if desired_project and rp != desired_project:
                continue
            if desired_database and rd != desired_database:
                continue
            seen.add(key)
            label = f"catalog:{rp}/{row.get('branch_name', '')}/{rd}"
            candidates.append((pid, bid, label))

        if not candidates:
            raise RuntimeError(
                "Failed to resolve SerenDB URL via logged-in Seren CLI context. "
                f"Could not infer a project/branch for {env_key}. "
                "Set `serendb.project_id` + `serendb.branch_id`, or provide WF_SERENDB_URL."
            )

        attempt_errors: list[str] = []
        for project_id, branch_id, source in candidates:
            cmd = [*base_cmd, "--project-id", project_id, "--branch-id", branch_id]
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if result.returncode == 0:
                resolved = _parse_dotenv_value(env_path, env_key).strip()
                if resolved:
                    os.environ[env_key] = resolved
                    logger.emit(
                        "serendb_url_resolved",
                        "Resolved SerenDB URL from Seren CLI context",
                        env_key=env_key,
                        source=f"seren_cli_context:{source}",
                    )
                    return resolved, f"seren_cli_context:{source}"
                attempt_errors.append(f"{source}: empty dotenv write")
                continue
            stderr = (result.stderr or "").strip()
            stdout = (result.stdout or "").strip()
            details = stderr or stdout or "unknown error"
            attempt_errors.append(f"{source}: {details}")

        preview = "; ".join(attempt_errors[:5])
        raise RuntimeError(
            "Failed to resolve SerenDB URL via logged-in Seren CLI context. "
            f"Tried {len(candidates)} candidates for {env_key}. "
            f"Errors: {preview}"
        )


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

QUERY_CATEGORIZED_TRANSACTIONS = """
SELECT
  t.row_hash,
  t.account_masked,
  t.txn_date,
  t.description_raw,
  t.amount,
  t.currency,
  COALESCE(c.category, 'uncategorized') AS category,
  COALESCE(c.category_source, 'none') AS category_source,
  c.confidence
FROM wf_transactions t
LEFT JOIN wf_txn_categories c ON c.row_hash = t.row_hash
WHERE t.txn_date >= %(start_date)s
  AND t.txn_date <= %(end_date)s
ORDER BY t.txn_date, t.row_hash
"""


def fetch_transactions(
    database_url: str,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                QUERY_CATEGORIZED_TRANSACTIONS,
                {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
            )
            columns = [desc.name for desc in cur.description]
            rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    return rows


# ---------------------------------------------------------------------------
# SerenDB persistence
# ---------------------------------------------------------------------------

def _read_sql(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return path.read_text(encoding="utf-8")


def persist_tax_summary(
    database_url: str,
    schema_path: Path,
    run_record: dict[str, Any],
    summary: dict[str, Any],
) -> None:
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(_read_sql(schema_path))

            cur.execute(
                """
                INSERT INTO wf_tax_runs (
                  run_id, started_at, ended_at, status,
                  tax_year, period_start, period_end,
                  total_income, total_deductible, total_non_deductible,
                  txn_count, artifact_root
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (run_id)
                DO UPDATE SET
                  ended_at = EXCLUDED.ended_at,
                  status = EXCLUDED.status,
                  total_income = EXCLUDED.total_income,
                  total_deductible = EXCLUDED.total_deductible,
                  total_non_deductible = EXCLUDED.total_non_deductible,
                  txn_count = EXCLUDED.txn_count
                """,
                (
                    run_record["run_id"],
                    run_record["started_at"],
                    run_record["ended_at"],
                    run_record["status"],
                    run_record["tax_year"],
                    run_record["period_start"],
                    run_record["period_end"],
                    summary["total_income"],
                    summary["total_deductible"],
                    summary["total_non_deductible"],
                    run_record["txn_count"],
                    run_record["artifact_root"],
                ),
            )

            # Insert line items
            for section in ("income", "deductible", "non_deductible"):
                items = summary.get(section, {})
                for key, item in items.items():
                    is_deductible = bool(item.get("is_deductible", section == "deductible"))
                    cur.execute(
                        """
                        INSERT INTO wf_tax_line_items (
                          run_id, section, category, label,
                          schedule, line_number, is_deductible,
                          amount, txn_count
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (run_id, section, category)
                        DO UPDATE SET
                          label = EXCLUDED.label,
                          schedule = EXCLUDED.schedule,
                          line_number = EXCLUDED.line_number,
                          is_deductible = EXCLUDED.is_deductible,
                          amount = EXCLUDED.amount,
                          txn_count = EXCLUDED.txn_count
                        """,
                        (
                            run_record["run_id"],
                            section,
                            key,
                            item["label"],
                            item.get("schedule", ""),
                            item.get("line", ""),
                            is_deductible,
                            item["amount"],
                            item["txn_count"],
                        ),
                    )

            # Insert snapshot
            snapshot_json = json.dumps(
                {
                    "income": summary["income"],
                    "deductible": summary["deductible"],
                    "non_deductible": summary["non_deductible"],
                },
                default=str,
            )
            cur.execute(
                """
                INSERT INTO wf_tax_snapshots (
                  run_id, tax_year, period_start, period_end,
                  total_income, total_deductible, total_non_deductible,
                  line_items_json
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                ON CONFLICT (run_id)
                DO UPDATE SET
                  total_income = EXCLUDED.total_income,
                  total_deductible = EXCLUDED.total_deductible,
                  total_non_deductible = EXCLUDED.total_non_deductible,
                  line_items_json = EXCLUDED.line_items_json
                """,
                (
                    run_record["run_id"],
                    run_record["tax_year"],
                    run_record["period_start"],
                    run_record["period_end"],
                    summary["total_income"],
                    summary["total_deductible"],
                    summary["total_non_deductible"],
                    snapshot_json,
                ),
            )

        conn.commit()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a Wells Fargo tax preparation summary from SerenDB transaction data",
    )
    parser.add_argument("--config", default="config.json", help="Path to config JSON")
    parser.add_argument(
        "--year",
        type=int,
        default=2025,
        help="Tax year (default 2025). Sets period to Jan 1 - Dec 31 of that year.",
    )
    parser.add_argument("--start", type=str, default="", help="Start date (YYYY-MM-DD), overrides --year")
    parser.add_argument("--end", type=str, default="", help="End date (YYYY-MM-DD), defaults to Dec 31 of --year")
    parser.add_argument("--out", type=str, default="artifacts/tax-prep", help="Output directory")
    parser.add_argument("--skip-persist", action="store_true", help="Skip SerenDB persistence")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Load config
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)
    config = json.loads(config_path.read_text(encoding="utf-8"))

    # Determine date range
    tax_year = args.year
    if args.start:
        period_start = date.fromisoformat(args.start)
        period_end = date.fromisoformat(args.end) if args.end else date(tax_year, 12, 31)
    else:
        period_start = date(tax_year, 1, 1)
        period_end = date(tax_year, 12, 31)

    # Set up output directories
    out_dir = Path(args.out)
    report_dir = ensure_dir(out_dir / "reports")
    export_dir = ensure_dir(out_dir / "exports")
    log_dir = ensure_dir(out_dir / "logs")

    run_id = f"tax-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
    logger = RunLogger(log_dir / f"{run_id}.jsonl")

    logger.emit("start", "Tax preparation generation started", run_id=run_id)
    logger.emit(
        "period",
        f"Tax year {tax_year}: {period_start.isoformat()} to {period_end.isoformat()}",
        tax_year=tax_year,
        start=period_start.isoformat(),
        end=period_end.isoformat(),
    )

    run_record: dict[str, Any] = {
        "run_id": run_id,
        "started_at": utc_now_iso(),
        "ended_at": None,
        "status": "running",
        "tax_year": tax_year,
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "txn_count": 0,
        "artifact_root": str(out_dir.resolve()),
    }

    try:
        # Resolve SerenDB URL
        db_url, db_source = resolve_serendb_database_url(config, logger)
        logger.emit("serendb_connected", f"Connected via {db_source}")

        # Fetch transactions
        logger.emit("query_transactions", "Fetching categorized transactions from SerenDB")
        transactions = fetch_transactions(db_url, period_start, period_end)
        logger.emit("query_transactions_done", f"Fetched {len(transactions)} transactions", count=len(transactions))

        if not transactions:
            logger.emit("warn", "No transactions found for the specified period. Is bank-statement-processing data synced?")
            run_record["status"] = "empty"
            run_record["ended_at"] = utc_now_iso()
            run_record["txn_count"] = 0
            print("No transactions found. Ensure bank-statement-processing has synced data to SerenDB.")
            sys.exit(0)

        run_record["txn_count"] = len(transactions)

        # Load tax categories map
        map_path_str = config.get("tax_map_path", "config/tax_categories.json")
        map_path = Path(map_path_str)
        if not map_path.is_absolute():
            map_path = config_path.parent / map_path
        tax_map = load_tax_categories(map_path)
        logger.emit("tax_map_loaded", f"Loaded tax categories map from {map_path}")

        # Build tax summary
        logger.emit("build_summary", "Building tax preparation summary")
        summary = build_tax_summary(transactions, tax_map)
        logger.emit(
            "build_summary_done",
            "Tax preparation summary built",
            total_income=summary["total_income"],
            total_deductible=summary["total_deductible"],
            total_non_deductible=summary["total_non_deductible"],
        )

        # Render reports
        md_content = render_markdown(summary, period_start, period_end, run_id, len(transactions), tax_year)
        md_path = report_dir / f"{run_id}.md"
        md_path.write_text(md_content, encoding="utf-8")

        json_report = {
            "run_id": run_id,
            "tax_year": tax_year,
            "period_start": period_start.isoformat(),
            "period_end": period_end.isoformat(),
            "txn_count": len(transactions),
            "summary": summary,
        }
        json_path = report_dir / f"{run_id}.json"
        dump_json(json_path, json_report)

        # Export line items
        export_path = export_dir / f"{run_id}.tax_items.jsonl"
        for section in ("income", "deductible", "non_deductible"):
            for key, item in summary.get(section, {}).items():
                append_jsonl(export_path, {
                    "section": section,
                    "category": key,
                    "label": item["label"],
                    "amount": item["amount"],
                    "txn_count": item["txn_count"],
                    "is_deductible": item.get("is_deductible", section == "deductible"),
                })

        logger.emit("render_done", "Reports written", md=str(md_path), json=str(json_path))

        # Persist to SerenDB
        if not args.skip_persist and bool(config.get("serendb", {}).get("enabled", True)):
            schema_path_str = config.get("serendb", {}).get("schema_path", "sql/schema.sql")
            schema_path = Path(schema_path_str)
            if not schema_path.is_absolute():
                schema_path = config_path.parent / schema_path
            logger.emit("persist", "Persisting tax preparation summary to SerenDB")
            run_record["status"] = "success"
            run_record["ended_at"] = utc_now_iso()
            persist_tax_summary(db_url, schema_path, run_record, summary)
            logger.emit("persist_done", "SerenDB persistence complete")
        else:
            run_record["status"] = "success"
            run_record["ended_at"] = utc_now_iso()
            logger.emit("persist_skipped", "SerenDB persistence skipped")

        logger.emit("complete", "Tax preparation generation complete")
        print(f"\nTax Preparation Summary generated successfully!")
        print(f"  Markdown: {md_path}")
        print(f"  JSON:     {json_path}")
        print(f"  Tax Year: {tax_year}")
        print(f"  Period:   {period_start} to {period_end}")
        print(f"  Transactions: {len(transactions)}")
        print(f"  Total Income:         ${summary['total_income']:,.2f}")
        print(f"  Total Deductible:     ${summary['total_deductible']:,.2f}")
        print(f"  Total Non-Deductible: ${summary['total_non_deductible']:,.2f}")

    except Exception as exc:
        run_record["status"] = "error"
        run_record["ended_at"] = utc_now_iso()
        logger.emit("error", str(exc), error_type=type(exc).__name__)
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
