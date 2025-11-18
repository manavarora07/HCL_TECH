"""Common ETL utilities used by other etl modules."""
from pathlib import Path
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]

def load_config(path: str = "configs/ingestion_config.yml"):
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)
    return yaml.safe_load(p.read_text())

def read_csv_to_df(path: str):
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(p)
    return pd.read_csv(p)

def ensure_data_dir():
    d = ROOT / "data"
    d.mkdir(parents=True, exist_ok=True)
    return d
