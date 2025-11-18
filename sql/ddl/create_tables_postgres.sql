-- SQL: create_tables_postgres.sql
-- Wide "raw" transactions table that mirrors your CSV columns.
-- Adapt types if you prefer tighter typing.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS raw_transactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),   -- internal surrogate id
    transaction_id TEXT,
    customer_id TEXT,
    name TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zipcode TEXT,
    country TEXT,
    department TEXT,
    item_price NUMERIC,
    quantity INTEGER,
    date TIMESTAMP,
    year INTEGER,
    month TEXT,
    time TEXT,
    total_purchases NUMERIC,
    amount NUMERIC,
    total_amount NUMERIC,
    product_category TEXT,
    product_brand TEXT,
    product_type TEXT,
    feedback TEXT,
    shipping_method TEXT,
    payment_method TEXT,
    order_status TEXT,
    ratings NUMERIC,
    products TEXT,
    created_at TIMESTAMP DEFAULT now()
);

-- Optional index examples to speed common queries
CREATE INDEX IF NOT EXISTS idx_raw_transactions_transaction_id ON raw_transactions(transaction_id);
CREATE INDEX IF NOT EXISTS idx_raw_transactions_customer_id ON raw_transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_raw_transactions_date ON raw_transactions(date);
