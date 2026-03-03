-- fetch_categorized_transactions: retrieve all categorized transactions for a date range
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
ORDER BY t.txn_date, t.row_hash;

-- fetch_cashflow_summary: retrieve cash flow totals by activity for a date range
SELECT
  activity,
  SUM(CASE WHEN direction = 'inflow' THEN amount ELSE 0 END) AS total_inflows,
  SUM(CASE WHEN direction = 'outflow' THEN amount ELSE 0 END) AS total_outflows,
  SUM(amount) AS net,
  COUNT(*) AS txn_count
FROM wf_cashflow_activities a
JOIN wf_cashflow_runs r ON r.run_id = a.run_id
WHERE r.status = 'success'
  AND r.period_start >= %(start_date)s
  AND r.period_end <= %(end_date)s
GROUP BY activity
ORDER BY activity;
