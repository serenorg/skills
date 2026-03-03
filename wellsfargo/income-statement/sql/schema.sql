CREATE TABLE IF NOT EXISTS wf_income_runs (
  run_id TEXT PRIMARY KEY,
  started_at TIMESTAMPTZ NOT NULL,
  ended_at TIMESTAMPTZ,
  status TEXT NOT NULL,
  period_start DATE NOT NULL,
  period_end DATE NOT NULL,
  total_income NUMERIC(14,2) NOT NULL DEFAULT 0,
  total_expenses NUMERIC(14,2) NOT NULL DEFAULT 0,
  net_income NUMERIC(14,2) NOT NULL DEFAULT 0,
  txn_count INTEGER NOT NULL DEFAULT 0,
  artifact_root TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wf_income_line_items (
  id SERIAL PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES wf_income_runs(run_id) ON DELETE CASCADE,
  section TEXT NOT NULL,
  line_item_key TEXT NOT NULL,
  label TEXT NOT NULL,
  amount NUMERIC(14,2) NOT NULL DEFAULT 0,
  txn_count INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (run_id, section, line_item_key)
);

CREATE INDEX IF NOT EXISTS idx_wf_income_line_items_run ON wf_income_line_items(run_id);

CREATE TABLE IF NOT EXISTS wf_income_snapshots (
  run_id TEXT PRIMARY KEY REFERENCES wf_income_runs(run_id) ON DELETE CASCADE,
  period_start DATE NOT NULL,
  period_end DATE NOT NULL,
  total_income NUMERIC(14,2) NOT NULL DEFAULT 0,
  total_expenses NUMERIC(14,2) NOT NULL DEFAULT 0,
  net_income NUMERIC(14,2) NOT NULL DEFAULT 0,
  line_items_json JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wf_income_snapshots_period ON wf_income_snapshots(period_start, period_end);

CREATE OR REPLACE VIEW v_wf_income_latest AS
SELECT s.*
FROM wf_income_snapshots s
JOIN wf_income_runs r ON r.run_id = s.run_id
WHERE r.status = 'success'
AND r.ended_at = (
  SELECT MAX(r2.ended_at)
  FROM wf_income_runs r2
  WHERE r2.status = 'success'
);

CREATE OR REPLACE VIEW v_wf_income_by_month AS
SELECT
  li.run_id,
  li.section,
  li.line_item_key,
  li.label,
  li.amount,
  li.txn_count,
  r.period_start,
  r.period_end
FROM wf_income_line_items li
JOIN wf_income_runs r ON r.run_id = li.run_id
WHERE r.status = 'success'
ORDER BY r.period_start DESC, li.section, li.line_item_key;
