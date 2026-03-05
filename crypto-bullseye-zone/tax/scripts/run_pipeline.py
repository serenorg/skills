#!/usr/bin/env python3
"""Run the full 1099-DA review and reconciliation pipeline and persist to SerenDB.

Supports three modes:
1. Single-file review (default): analyze 1099-DA only, no second data source.
2. Kraken API verification: fetch raw trades via Kraken API and reconcile.
3. Legacy tax-software input: compare against tax software export (backward compatible).
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from common import load_records, stable_id, write_json
from cost_basis_resolver import resolve
from reconciliation_audit import audit, normalize_kraken_trades, normalize_tax_rows
from serendb_store import persist_artifacts


def _load_normalizer_module():
    module_path = Path(__file__).with_name("1099da_normalizer.py")
    spec = importlib.util.spec_from_file_location("normalizer_1099da", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load 1099da_normalizer.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules["normalizer_1099da"] = module
    spec.loader.exec_module(module)
    return module


def _fetch_kraken_trades(api_key: str, api_secret: str, tax_year: Optional[int]) -> List[Dict[str, Any]]:
    """Fetch and normalize trades from Kraken API."""
    from kraken_api_fetcher import fetch_trades, normalize_trades

    start_ts = None
    end_ts = None
    if tax_year:
        from datetime import datetime, timezone

        start_ts = int(datetime(tax_year, 1, 1, tzinfo=timezone.utc).timestamp())
        end_ts = int(datetime(tax_year + 1, 1, 1, tzinfo=timezone.utc).timestamp())

    print("Fetching trades from Kraken API...")
    raw_trades = fetch_trades(api_key, api_secret, start=start_ts, end=end_ts)
    print(f"  Fetched {len(raw_trades)} raw trades")

    normalized = normalize_trades(raw_trades)
    print(f"  Normalized {len(normalized)} sell dispositions")
    return normalized


def main() -> None:
    parser = argparse.ArgumentParser(description="Run 1099-DA review and reconciliation pipeline")
    parser.add_argument("--input-1099da", required=True, help="1099-DA CSV/JSON/JSONL path")
    parser.add_argument("--input-tax", default=None, help="Tax software CSV/JSON/JSONL path (legacy, optional)")
    parser.add_argument("--kraken-api-key", default=None, help="Kraken API key for transaction verification")
    parser.add_argument("--kraken-api-secret", default=None, help="Kraken API secret for transaction verification")
    parser.add_argument("--tax-year", type=int, default=None, help="Tax year to filter Kraken API results")
    parser.add_argument("--output-dir", required=True, help="Output directory for JSON artifacts")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Normalize and resolve 1099-DA
    normalizer = _load_normalizer_module()
    normalized = normalizer.normalize_rows(load_records(args.input_1099da))
    resolved = resolve(normalized)

    write_json(str(output_dir / "normalized_1099da.json"), {"count": len(normalized), "records": normalized})
    write_json(str(output_dir / "resolved_lots.json"), {"count": len(resolved), "records": resolved})

    # Step 2: Determine reconciliation mode
    comparison_rows: List[Dict[str, Any]] = []
    mode = "review_only"
    input_tax_path = None

    if args.kraken_api_key and args.kraken_api_secret:
        kraken_dispositions = _fetch_kraken_trades(args.kraken_api_key, args.kraken_api_secret, args.tax_year)
        comparison_rows = normalize_kraken_trades(kraken_dispositions)
        write_json(str(output_dir / "kraken_trades.json"), {
            "count": len(kraken_dispositions),
            "dispositions": kraken_dispositions,
        })
        mode = "kraken_api"
        input_tax_path = "kraken_api"
    elif args.input_tax:
        comparison_rows = normalize_tax_rows(load_records(args.input_tax))
        mode = "tax_software"
        input_tax_path = args.input_tax

    # Step 3: Run audit if we have comparison data
    summary: Dict[str, Any]
    exceptions: List[Dict[str, Any]]

    if comparison_rows:
        summary, exceptions = audit(resolved_rows=resolved, tax_rows=comparison_rows)
    else:
        summary = {
            "mode": "review_only",
            "total_dispositions": len(resolved),
            "total_proceeds_usd": round(sum(r.get("proceeds_usd") or 0.0 for r in resolved), 2),
            "total_basis_usd": round(sum(r.get("cost_basis_usd") or 0.0 for r in resolved), 2),
            "total_gain_loss_usd": round(sum(r.get("gain_loss_usd") or 0.0 for r in resolved), 2),
            "missing_basis_count": sum(1 for r in resolved if not r.get("cost_basis_usd")),
            "missing_holding_period_count": sum(1 for r in resolved if not r.get("holding_period")),
        }
        exceptions = []
        for r in resolved:
            if not r.get("cost_basis_usd"):
                exceptions.append({
                    "id": r.get("record_id"),
                    "asset": r.get("asset"),
                    "date_time": r.get("disposed_at"),
                    "delta": None,
                    "likely_cause": "missing_cost_basis",
                    "recommended_fix": "Provide acquisition records or contact your exchange for transfer history.",
                    "status": "open",
                })

    write_json(
        str(output_dir / "reconciliation_audit.json"),
        {"summary": summary, "exceptions": exceptions, "mode": mode},
    )

    # Step 4: Persist to SerenDB
    run_id = stable_id([args.input_1099da, input_tax_path or "review_only", len(normalized)])

    persistence = persist_artifacts(
        run_id=run_id,
        normalized=normalized,
        resolved=resolved,
        exceptions=exceptions,
        summary=summary,
        input_1099da_path=args.input_1099da,
        input_tax_path=input_tax_path or "",
    )

    pipeline_result: Dict[str, Any] = {
        "run_id": run_id,
        "mode": mode,
        "summary": summary,
        "exceptions": exceptions,
        "persistence": persistence,
    }
    write_json(str(output_dir / "pipeline_result.json"), pipeline_result)
    print(json.dumps(pipeline_result, indent=2))


if __name__ == "__main__":
    main()
