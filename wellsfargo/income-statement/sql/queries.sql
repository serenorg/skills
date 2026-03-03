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

-- fetch_monthly_summary: retrieve monthly aggregates for a date range
SELECT
  account_masked,
  month_start,
  debit_total,
  credit_total,
  txn_count
FROM wf_monthly_summary
WHERE month_start >= %(start_date)s
  AND month_start <= %(end_date)s
ORDER BY month_start;
