---
name: tax-prep
description: "Categorize transactions into IRS tax line items and generate tax preparation summaries from Wells Fargo data in SerenDB."
---

# Wells Fargo Tax Prep

## When To Use

- Categorize transactions into IRS Schedule C or standard tax deduction categories.
- Generate tax preparation summaries with deductible expenses.
- Estimate potential tax deductions from bank transaction data.
- Persist tax categorization snapshots into SerenDB for accountant review.

## Prerequisites

- The `bank-statement-processing` skill must have completed at least one successful run with SerenDB sync enabled.
- SerenDB must contain populated `wf_transactions` and `wf_txn_categories` tables.

## Safety Profile

- Read-only against SerenDB source tables.
- Writes only to dedicated `wf_tax_*` tables (never modifies upstream data).
- Not a substitute for professional tax advice. Generated summaries are estimates only.

## Quick Start

```bash
cd wellsfargo/tax-prep
python3 -m pip install -r requirements.txt
cp .env.example .env && cp config.example.json config.json
python3 scripts/run.py --config config.json --year 2025 --out artifacts/tax-prep
```

## Commands

```bash
python3 scripts/run.py --config config.json --year 2025 --out artifacts/tax-prep
python3 scripts/run.py --config config.json --start 2025-01-01 --end 2025-12-31 --out artifacts/tax-prep
python3 scripts/run.py --config config.json --year 2025 --skip-persist --out artifacts/tax-prep
```

## Outputs

- Markdown report: `artifacts/tax-prep/reports/<run_id>.md`
- JSON report: `artifacts/tax-prep/reports/<run_id>.json`
- Line items: `artifacts/tax-prep/exports/<run_id>.tax_items.jsonl`

## SerenDB Tables

- `wf_tax_runs` - tax prep runs
- `wf_tax_line_items` - per-category tax line items per run
- `wf_tax_snapshots` - summary snapshot per run

## Reusable Views

- `v_wf_tax_latest` - most recent tax prep snapshot
- `v_wf_tax_deductions` - deductible items from latest run
