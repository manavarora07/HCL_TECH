-- Example transform for SQLite.
-- Creates `transactions_enriched` table by copying transactions and adding a simple flag.
DROP TABLE IF EXISTS transactions_enriched;
CREATE TABLE transactions_enriched AS
SELECT
  transaction_id,
  customer_id,
  amount,
  ts,
  CASE WHEN amount >= 100 THEN 1 ELSE 0 END as big_spend
FROM transactions;
