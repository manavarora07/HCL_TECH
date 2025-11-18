#!/usr/bin/env python3
"""
etl/load_to_postgres.py

Usage:
    python etl/load_to_postgres.py --csv sample_data/retail_data_Source.csv

Environment variables:
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
or pass connection args via command-line.

What it does:
- Runs SQL file sql/ddl/create_tables_postgres.sql to ensure raw_transactions exists.
- Loads CSV into a temporary table using COPY, then INSERTs into raw_transactions,
  coercing/renaming columns where necessary.
"""
import argparse
import os
import psycopg2
from pathlib import Path
import pandas as pd
import io
import sys
import textwrap
import subprocess

REPO_ROOT = Path(__file__).resolve().parents[1]
DDL_FILE = REPO_ROOT / "sql" / "ddl" / "create_tables_postgres.sql"

DEFAULT_CSV = REPO_ROOT / "sample_data" / "retail_data_Source.csv"

# mapping from CSV headers -> table columns (lowercased compare)
# if your CSV uses slightly different names, adjust mapping here
PREFERRED_COL_MAP = {
    "transaction_id": "transaction_id",
    "customer_id": "customer_id",
    "name": "name",
    "email": "email",
    "phone": "phone",
    "address": "address",
    "city": "city",
    "state": "state",
    "zipcode": "zipcode",
    "country": "country",
    "department": "department",
    "item_price": "item_price",
    "quantity": "quantity",
    "date": "date",
    "year": "year",
    "month": "month",
    "time": "time",
    "total_purchases": "total_purchases",
    "amount": "amount",
    "total_amount": "total_amount",
    "product_category": "product_category",
    "product_brand": "product_brand",
    "product_type": "product_type",
    "feedback": "feedback",
    "shipping_method": "shipping_method",
    "payment_method": "payment_method",
    "order_status": "order_status",
    "ratings": "ratings",
    "products": "products"
}

def run_sql_file(conn, path: Path):
    sql = path.read_text()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

def create_table_and_load(conn, csv_path: Path):
    # Ensure DDL executed
    run_sql_file(conn, DDL_FILE)

    # Read CSV header to align column names
    df = pd.read_csv(csv_path, nrows=0)
    csv_cols = [c for c in df.columns]

    # Build mapping from csv column to table column (if found)
    mapping = []
    for c in csv_cols:
        key = c.strip().lower()
        if key in PREFERRED_COL_MAP:
            mapping.append((c, PREFERRED_COL_MAP[key]))
        else:
            # Unknown column -> keep as is into 'products' or ignore
            mapping.append((c, c))

    # Read whole CSV as text and use COPY to a temp table with matching columns
    # For simplicity we'll COPY into a temporary table with all columns as text, then INSERT selecting columns we want.
    tmp_table = "tmp_raw_csv"
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {tmp_table}")
        # Create temp table with text columns for all CSV headers
        cols_sql = ", ".join([f"\"{c}\" TEXT" for c, _ in mapping])
        cur.execute(f"CREATE TEMP TABLE {tmp_table} ({cols_sql}) ON COMMIT DROP;")
        conn.commit()

    # Use COPY FROM STDIN for the temp table
    with open(csv_path, "r", encoding="utf-8") as fh:
        with conn.cursor() as cur:
            # Build COPY command using header names exactly
            cols_list = ",".join([f"\"{c}\"" for c, _ in mapping])
            copy_sql = f"COPY {tmp_table}({cols_list}) FROM STDIN WITH CSV HEADER DELIMITER ',' NULL ''"
            cur.copy_expert(copy_sql, fh)
        conn.commit()

    # Insert from temp into raw_transactions selecting/casting columns
    # Prepare select list using mapping: use COALESCE(NULLIF(trim(col),''),'') as appropriate.
    select_parts = []
    for csv_col, tbl_col in mapping:
        # only insert into columns that exist in raw_transactions (we know list from DDL)
        # insert as-is with trimming
        select_parts.append(f"trim(NULLIF({tmp_table}.\"{csv_col}\",'') ) AS {tbl_col}")

    select_clause = ",\n    ".join(select_parts)

    insert_sql = textwrap.dedent(f"""
        INSERT INTO raw_transactions (
            transaction_id, customer_id, name, email, phone, address, city, state, zipcode, country,
            department, item_price, quantity, date, year, month, time, total_purchases, amount, total_amount,
            product_category, product_brand, product_type, feedback, shipping_method, payment_method, order_status, ratings, products
        )
        SELECT
            {select_clause}
        FROM {tmp_table};
    """)

    with conn.cursor() as cur:
        cur.execute(insert_sql)
    conn.commit()

def get_conn(args):
    conn = psycopg2.connect(
        host=args.host or os.environ.get("PGHOST", "localhost"),
        port=args.port or int(os.environ.get("PGPORT", 5432)),
        dbname=args.dbname or os.environ.get("PGDATABASE", "loyalty"),
        user=args.user or os.environ.get("PGUSER", "postgres"),
        password=args.password or os.environ.get("PGPASSWORD", "example"),
    )
    return conn

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default=str(DEFAULT_CSV))
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--dbname")
    parser.add_argument("--user")
    parser.add_argument("--password")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print("CSV not found:", csv_path)
        sys.exit(2)

    conn = get_conn(args)
    try:
        create_table_and_load(conn, csv_path)
        print("Load complete.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
