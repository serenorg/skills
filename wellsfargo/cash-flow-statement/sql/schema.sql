CREATE TABLE IF NOT EXISTS wf_cashflow_runs (
  run_id TEXT PRIMARY KEY,
  started_at TIMESTAMPTZ NOT NULL,
  ended_at TIMESTAMPTZ,
  status TEXT NOT NULL,
  period_start DATE NOT NULL,
  period_end DATE NOT NULL,
  operating_net NUMERIC(14,2) NOT NULL DEFAULT 0,
  investing_net NUMERIC(14,2) NOT NULL DEFAULT 0,
  financing_net NUMERIC(14,2) NOT NULL DEFAULT 0,
  net_cash_change NUMERIC(14,2) NOT NULL DEFAULT 0,
  txn_count INTEGER NOT NULL DEFAULT 0,
  artifact_root TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS wf_cashflow_activities (
  id SERIAL PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES wf_cashflow_runs(run_id) ON DELETE CASCADE,
  activity TEXT NOT NULL,
  line_item_key TEXT NOT NULL,
  label TEXT NOT NULL,
  direction TEXT NOT NULL,
  amount NUMERIC(14,2) NOT NULL DEFAULT 0,
  txn_count INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (run_id, activity, line_item_key)
);

CREATE INDEX IF NOT EXISTS idx_wf_cashflow_activities_run ON wf_cashflow_activities(run_id);

CREATE TABLE IF NOT EXISTS wf_cashflow_snapshots (
  run_id TEXT PRIMARY KEY REFERENCES wf_cashflow_runs(run_id) ON DELETE CASCADE,
  period_start DATE NOT NULL,
  period_end DATE NOT NULL,
  operating_net NUMERIC(14,2) NOT NULL DEFAULT 0,
  investing_net NUMERIC(14,2) NOT NULL DEFAULT 0,
  financing_net NUMERIC(14,2) NOT NULL DEFAULT 0,
  net_cash_change NUMERIC(14,2) NOT NULL DEFAULT 0,
  activities_json JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wf_cashflow_snapshots_period ON wf_cashflow_snapshots(period_start, period_end);

CREATE OR REPLACE VIEW v_wf_cashflow_latest AS
SELECT s.*
FROM wf_cashflow_snapshots s
JOIN wf_cashflow_runs r ON r.run_id = s.run_id
WHERE r.status = 'success'
AND r.ended_at = (
  SELECT MAX(r2.ended_at)
  FROM wf_cashflow_runs r2
  WHERE r2.status = 'success'
);

CREATE OR REPLACE VIEW v_wf_cashflow_by_month AS
SELECT
  a.run_id,
  a.activity,
  a.line_item_key,
  a.label,
  a.direction,
  a.amount,
  a.txn_count,
  r.period_start,
  r.period_end
FROM wf_cashflow_activities a
JOIN wf_cashflow_runs r ON r.run_id = a.run_id
WHERE r.status = 'success'
ORDER BY r.period_start DESC, a.activity, a.line_item_key;
