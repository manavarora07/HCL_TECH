"""
transform.py

Simple transform runner that:
- Loads staged CSV (data/staged.csv) into a local SQLite DB at data/loyalty.db
- Runs SQL files in `sql/transforms/` in alphabetical order
- Writes results back as tables in the SQLite DB (so you can inspect via sqlite browser)

Usage:
    python etl/transform.py              # uses data/staged.csv
    python etl/transform.py --csv path/to/file.csv
"""
from pathlib import Path
import sqlite3
import pandas as pd
import glob

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CSV = ROOT / "data" / "staged.csv"
DB_PATH = ROOT / "data" / "loyalty.db"
TRANSFORMS_DIR = ROOT / "sql" / "transforms"

def csv_to_sqlite_table(csv_path: Path, conn: sqlite3.Connection, table_name: str = "transactions"):
    df = pd.read_csv(csv_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    print(f"Wrote {len(df)} rows -> {table_name}")

def run_sql_scripts(conn: sqlite3.Connection, transforms_dir: Path):
    sql_files = sorted(transforms_dir.glob("*.sql"))
    for sql_file in sql_files:
        sql_text = sql_file.read_text()
        try:
            print(f"Running transform: {sql_file.name}")
            # For safety, use executescript to allow multiple statements
            conn.executescript(sql_text)
        except Exception as e:
            print(f"Error running {sql_file.name}: {e}")
            raise

def main(csv_path: Path = DEFAULT_CSV):
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}. Run etl/ingest.py first to stage it.")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    try:
        csv_to_sqlite_table(csv_path, conn, table_name="transactions")
        run_sql_scripts(conn, TRANSFORMS_DIR)
        print("Transforms completed. You can inspect the SQLite DB at:", DB_PATH)
    finally:
        conn.commit()
        conn.close()

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--csv", default=str(DEFAULT_CSV))
    args = p.parse_args()
    main(Path(args.csv))
