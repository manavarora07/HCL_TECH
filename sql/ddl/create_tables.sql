-- DDL for a simplest transactions table (used as an example)
CREATE TABLE IF NOT EXISTS transactions (
  transaction_id TEXT PRIMARY KEY,
  customer_id TEXT,
  amount REAL,
  ts TEXT
);
