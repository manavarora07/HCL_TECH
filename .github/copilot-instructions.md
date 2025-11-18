Below are project-specific instructions to help AI coding agents contribute safely and productively to the "loyalty-pipeline" repository.

Keep this short and actionable — reference precise files and concrete patterns found in the codebase.

1. Big picture
   - This repo is a small, local-first ETL prototype for a loyalty/transactions pipeline. Key components:
     - `etl/` — ingest, validate, transform and small utilities (main developer surface).
     - `app/admin_service/app.py` — minimal Flask admin API used to expose health, staged CSV download and a validation endpoint.
     - `sql/transforms/` — SQLite-friendly SQL transform scripts (run by `etl/transform.py`).
     - `data/` (created at runtime) — destination for `staged.csv` and `loyalty.db` SQLite DB.
     - `configs/ingestion_config.yml` — canonical config: column list, `required` fields and `unique_keys` used by validation.

2. How code is structured and why
   - Scripts are CLI-first (not packaged). Expect to run modules directly: `python etl/ingest.py`, `python etl/validate.py`, `python etl/transform.py`.
   - Validation prefers Great Expectations if importable; otherwise it falls back to a lightweight pandas-based validator. Use `validate.run_validation(csv_path)` as the single public API.
   - Transform stage writes to a local SQLite DB (`data/loyalty.db`) for easy inspection; SQL scripts in `sql/transforms/` are executed in alphabetical order by filename.
   - Airflow/Prefect DAGs/flows exist as import-safe placeholders — guard runtime imports before changing (see `dags/example_dag.py`, `flows/example_flows.py`).

3. Developer workflows & useful commands
   - Ingest a CSV and optionally validate:
     - python etl/ingest.py path/to/file.csv --dest data/staged.csv --validate
   - Run validation directly (CLI or from tests):
     - python -m etl.validate sample_data/sample_transactions.csv
     - In Python: from etl import validate; validate.run_validation('data/staged.csv')
   - Run transforms and inspect results in SQLite:
     - python etl/transform.py --csv data/staged.csv
     - DB file: data/loyalty.db (open with sqlite3 or DB browser)
   - Run tests (pytest recommended): tests exercise `validate.run_validation` and assume repository root on sys.path.

4. Project-specific conventions and patterns
   - File paths are relative to repo root and many modules assume `ROOT = Path(__file__).resolve().parents[1]`. Avoid changing that root calculation unless refactoring consistently.
   - Validation config location is `configs/ingestion_config.yml`. Do not hardcode different config paths without updating `etl/utils.load_config` or `etl/validate` helper functions.
   - Validation function `run_validation(csv_path)` returns a boolean and writes JSON results to `validation_results/` when run. Tests assert boolean True/False — preserve this shape.
   - The codebase uses defensive, import-guarded integration points for optional dependencies (Great Expectations, Airflow, Prefect). New features that require optional libs should follow the same pattern (try/except import and graceful fallback).
   - SQL scripts assume SQLite dialect (e.g., `executescript` usage). If adding transforms, keep SQL compatible with SQLite or adapt `transform.py` to use a different engine.

5. Integration points & external dependencies
   - Optional: Great Expectations — if available, validation will use it. Tests do not require GE; keep lightweight validator stable.
   - The admin service (`app/admin_service/app.py`) imports `etl.validate` dynamically at runtime. Avoid circular imports; import `etl` modules inside endpoints as currently implemented.
   - No external data stores are required by default — everything is file-based (CSV + SQLite).

6. Examples to reference when making changes
   - How to stage data: `etl/ingest.py::copy_to_dest`
   - Lightweight validation example and GE fallback: `etl/validate.py` (look for `HAS_GE` and `_validate_lightweight`)
   - Running transforms into SQLite: `etl/transform.py::run_sql_scripts` and `sql/transforms/01_enrich.sql`
   - Admin API pattern (import inside endpoint, returns JSON/errors): `app/admin_service/app.py`

7. Testing & stability notes
   - Tests in `tests/` expect the repository root on sys.path and will call `validate.run_validation` directly against a staged/temp CSV. Keep this behavior when refactoring.
   - Preserve return values and side-effect behavior (writing to `validation_results/` and `data/`) unless tests are updated.

8. What not to change without CI or owner confirmation
   - Changing the `configs/ingestion_config.yml` shape (keys: `columns`, `required`, `unique_keys`). Many parts of the code rely on these exact keys.
   - Converting SQL scripts to a non-SQLite dialect without updating `etl/transform.py` and tests.

If any section is unclear or you'd like more examples (unit test snippets, more file references), tell me which area to expand and I'll iterate. 
