# Architecture (Overview)

Pipeline stages:
1. Ingest: copy/receive CSVs into a staging area.
2. Validate: ensure required columns, no nulls in required fields, unique keys.
   - Uses Great Expectations if available, otherwise a lightweight check.
3. Transform: run SQL transforms (optionally using SQLite or dbt).
4. Load: write results to a target DB (Postgres in docker-compose).
5. Serve: optional Flask admin service.

Local dev:
- venv + pip install -r requirements.txt
- Run `python etl/ingest.py sample_data/sample_transactions.csv --validate`
- Run transforms: `python etl/transform.py`
