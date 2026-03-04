# CARF / DAC8 Crypto Asset Reporting

Reconcile CARF/DAC8 CASP-reported transactions against your own records.

## Quick Start

1. `cp .env.example .env`
2. `cp config.example.json config.json`
3. `pip install -r requirements.txt`
4. Run:

```bash
python scripts/agent.py run \
  --config config.json \
  --carf-report tests/fixtures/sample_carf.xml \
  --user-records tests/fixtures/sample_transactions.csv \
  --accept-risk-disclaimer
```

Optional 1099-DA bridge:

```bash
python scripts/agent.py run \
  --config config.json \
  --carf-report tests/fixtures/sample_carf.xml \
  --user-records tests/fixtures/sample_transactions.csv \
  --bridge-1099da tests/fixtures/sample_1099da.csv \
  --accept-risk-disclaimer
```

Outputs are written to `state/reports/`:
- `reconciliation_report_<session_id>.md`
- `reconciliation_summary_<session_id>.json`
- `cpa_escalation_<session_id>.md` (only when needed)

## Tests

```bash
pytest -q tests
```
