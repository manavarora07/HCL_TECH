-- SQL: create_normalized_postgres.sql
-- Normalized schema: customers, orders, order_items, products (basic)

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    name TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zipcode TEXT,
    country TEXT
);

-- products table (minimal)
CREATE TABLE IF NOT EXISTS products (
    product_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    product_name TEXT,
    product_brand TEXT,
    product_type TEXT,
    product_category TEXT
);

-- orders table (one row per transaction)
CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id TEXT,
    customer_id TEXT REFERENCES customers(customer_id),
    order_date TIMESTAMP,
    total_purchases NUMERIC,
    amount NUMERIC,
    total_amount NUMERIC,
    shipping_method TEXT,
    payment_method TEXT,
    order_status TEXT,
    ratings NUMERIC
);

-- order_items table (one row per line item)
CREATE TABLE IF NOT EXISTS order_items (
    item_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id UUID REFERENCES orders(order_id),
    product_id UUID REFERENCES products(product_id),
    department TEXT,
    item_price NUMERIC,
    quantity INTEGER,
    feedback TEXT
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_orders_transaction_id ON orders(transaction_id);
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);
