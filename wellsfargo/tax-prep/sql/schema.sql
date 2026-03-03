CREATE TABLE IF NOT EXISTS wf_tax_runs (
  run_id TEXT PRIMARY KEY,
  started_at TIMESTAMPTZ NOT NULL,
  ended_at TIMESTAMPTZ,
  status TEXT NOT NULL,
  tax_year INTEGER NOT NULL,
  period_start DATE NOT NULL,
  period_end DATE NOT NULL,
  total_income NUMERIC(14,2) NOT NULL DEFAULT 0,
  total_deductible NUMERIC(14,2) NOT NULL DEFAULT 0,
  total_non_deductible NUMERIC(14,2) NOT NULL DEFAULT 0,
  txn_count INTEGER NOT NULL DEFAULT 0,
  artifact_root TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wf_tax_line_items (
  id SERIAL PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES wf_tax_runs(run_id) ON DELETE CASCADE,
  section TEXT NOT NULL,
  category TEXT NOT NULL,
  label TEXT NOT NULL,
  schedule TEXT NOT NULL DEFAULT '',
  line_number TEXT NOT NULL DEFAULT '',
  is_deductible BOOLEAN NOT NULL DEFAULT FALSE,
  amount NUMERIC(14,2) NOT NULL DEFAULT 0,
  txn_count INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (run_id, section, category)
);

CREATE INDEX IF NOT EXISTS idx_wf_tax_line_items_run ON wf_tax_line_items(run_id);

CREATE TABLE IF NOT EXISTS wf_tax_snapshots (
  run_id TEXT PRIMARY KEY REFERENCES wf_tax_runs(run_id) ON DELETE CASCADE,
  tax_year INTEGER NOT NULL,
  period_start DATE NOT NULL,
  period_end DATE NOT NULL,
  total_income NUMERIC(14,2) NOT NULL DEFAULT 0,
  total_deductible NUMERIC(14,2) NOT NULL DEFAULT 0,
  total_non_deductible NUMERIC(14,2) NOT NULL DEFAULT 0,
  line_items_json JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE VIEW v_wf_tax_latest AS
SELECT s.* FROM wf_tax_snapshots s
JOIN wf_tax_runs r ON r.run_id = s.run_id
WHERE r.status = 'success'
AND r.ended_at = (SELECT MAX(r2.ended_at) FROM wf_tax_runs r2 WHERE r2.status = 'success');

CREATE OR REPLACE VIEW v_wf_tax_deductions AS
SELECT li.* FROM wf_tax_line_items li
JOIN wf_tax_runs r ON r.run_id = li.run_id
WHERE r.status = 'success' AND li.is_deductible = TRUE
AND r.ended_at = (SELECT MAX(r2.ended_at) FROM wf_tax_runs r2 WHERE r2.status = 'success')
ORDER BY li.amount DESC;
