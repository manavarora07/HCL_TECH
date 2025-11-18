-- RFM-like aggregate (SQLite-compatible)
SELECT
  customer_id,
  COUNT(*) AS frequency,
  SUM(amount) AS monetary,
  MAX(ts) AS recency
FROM transactions
GROUP BY customer_id
ORDER BY monetary DESC;
