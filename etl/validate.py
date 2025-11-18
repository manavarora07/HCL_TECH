#!/usr/bin/env python3
"""
etl/validate.py  (lightweight, NO Great Expectations) with STRICT checks

Features:
- Loads config from configs/ingestion_config.yml
- Validates:
    * required columns exist
    * required columns have no nulls
    * unique keys are unique
    * simple data-type coercion checks (string, int, float, timestamp)
    * STRICT format checks:
        - Email: strict regex
        - Date: exact YYYY-MM-DD
        - Time: exact HH:MM:SS (24h)
- Writes JSON report to validation_results/<csv_stem>_result.json
- CLI:
    python etl/validate.py path/to/file.csv [--no-save] [--raise-on-fail]
"""
from pathlib import Path
import argparse
import json
import sys
import yaml
import traceback
import re
from datetime import datetime

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "configs" / "ingestion_config.yml"
VALIDATION_DIR = REPO_ROOT / "validation_results"


class ValidationError(Exception):
    pass


def load_config():
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    # normalize structure
    columns = cfg.get("columns", [])
    col_types = {}
    for c in columns:
        if isinstance(c, dict) and "name" in c:
            col_types[str(c["name"])] = str(c.get("type", "string")).lower()
        elif isinstance(c, str):
            col_types[c] = "string"
    required = cfg.get("required", []) or []
    unique_keys = cfg.get("unique_keys", []) or []
    # optional: custom strict formats (not required)
    validations = cfg.get("validations", {}) or {}
    # Example validations could be:
    # validations:
    #   Email: email
    #   Date: YYYY-MM-DD
    #   Time: HH:MM:SS
    return {"col_types": col_types, "required": list(required), "unique_keys": list(unique_keys), "validations": validations}


# --- Strict format check helpers ---

# Email regex: conservative but strict - allows common emails and rejects obvious bad ones.
_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

def validate_email_strict(value: str) -> bool:
    """Return True if value matches strict email pattern."""
    if value is None:
        return False
    v = str(value).strip()
    if v == "":
        return False
    return bool(_EMAIL_RE.match(v))


def validate_date_strict(value: str, fmt: str = "%Y-%m-%d") -> bool:
    """
    Strictly validate date using datetime.strptime.
    Default format: YYYY-MM-DD
    """
    if value is None:
        return False
    v = str(value).strip()
    if v == "":
        return False
    try:
        # this both checks format and that the date exists
        datetime.strptime(v, fmt)
        return True
    except Exception:
        return False


def validate_time_strict(value: str, fmt: str = "%H:%M:%S") -> bool:
    """
    Strictly validate time using datetime.strptime.
    Default format: HH:MM:SS (24-hour)
    """
    if value is None:
        return False
    v = str(value).strip()
    if v == "":
        return False
    try:
        datetime.strptime(v, fmt)
        return True
    except Exception:
        return False


# --- Type coercion helper used previously ---
def _coerce_and_check_series(df_col, desired_type):
    """
    Try to coerce a pandas Series to desired_type.
    Return dict with keys: success (bool), failed_count (int), sample_values (list)
    """
    import pandas as pd
    res = {"success": True, "failed_count": 0, "sample_values": []}
    ser = df_col
    if desired_type in ("string", "str", "object"):
        return res
    if desired_type in ("int", "integer"):
        coerced = pd.to_numeric(ser, errors="coerce")
        failed_mask = coerced.isna() & ser.notna() & (ser.str.strip() != "")
        failed = ser[failed_mask]
        res["failed_count"] = int(failed.shape[0])
        res["sample_values"] = failed.head(10).astype(str).tolist()
        res["success"] = res["failed_count"] == 0
        return res
    if desired_type in ("float", "numeric", "number"):
        coerced = pd.to_numeric(ser, errors="coerce")
        failed_mask = coerced.isna() & ser.notna() & (ser.str.strip() != "")
        failed = ser[failed_mask]
        res["failed_count"] = int(failed.shape[0])
        res["sample_values"] = failed.head(10).astype(str).tolist()
        res["success"] = res["failed_count"] == 0
        return res
    if desired_type in ("timestamp", "datetime", "date"):
        coerced = None
        try:
            coerced = pd.to_datetime(ser, errors="coerce")
        except Exception:
            # fallback: mark as all failed if parsing raises
            failed_mask = ser.notna() & (ser.str.strip() != "")
            failed = ser[failed_mask]
            res["failed_count"] = int(failed.shape[0])
            res["sample_values"] = failed.head(10).astype(str).tolist()
            res["success"] = False
            return res
        failed_mask = coerced.isna() & ser.notna() & (ser.str.strip() != "")
        failed = ser[failed_mask]
        res["failed_count"] = int(failed.shape[0])
        res["sample_values"] = failed.head(10).astype(str).tolist()
        res["success"] = res["failed_count"] == 0
        return res
    return res


# --- Main validator ---
def validate_csv(csv_path: str, save_result: bool = True):
    """
    Run validations and return a result dict:
      {
        success: bool,
        statistics: {...},
        results: [ ... failing expectations ... ],
        meta: {csv: ..., config: ...}
      }
    """
    import pandas as pd

    csv_p = Path(csv_path)
    if not csv_p.exists():
        raise FileNotFoundError(csv_p)

    cfg = load_config()
    col_types = cfg["col_types"]
    required = cfg["required"]
    unique_keys = cfg["unique_keys"]
    validations_cfg = cfg.get("validations", {})

    # read all as strings (safe)
    df = pd.read_csv(csv_p, dtype=str)
    total_rows = int(df.shape[0])

    failing = []

    # 1) required columns exist
    for col in required:
        if col not in df.columns:
            failing.append({
                "expectation": "expect_column_to_exist",
                "column": col,
                "success": False,
                "reason": "column missing"
            })

    # 2) required columns not null (empty string or NaN)
    present_required = [c for c in required if c in df.columns]
    for col in present_required:
        missing_mask = df[col].isna() | df[col].str.strip().eq("")
        missing_count = int(missing_mask.sum())
        if missing_count > 0:
            failing.append({
                "expectation": "expect_column_values_to_not_be_null",
                "column": col,
                "success": False,
                "result": {"unexpected_count": missing_count, "sample_unexpected_values": df.loc[missing_mask, col].head(10).astype(str).tolist()}
            })

    # 3) unique keys
    for key in unique_keys:
        if key not in df.columns:
            failing.append({
                "expectation": "expect_column_to_exist",
                "column": key,
                "success": False,
                "reason": "column missing (for uniqueness check)"
            })
            continue
        dup_mask = df[key].duplicated(keep=False)
        dup_count = int(dup_mask.sum())
        if dup_count > 0:
            dup_values = df.loc[dup_mask, key].unique().tolist()[:20]
            failing.append({
                "expectation": "expect_column_values_to_be_unique",
                "column": key,
                "success": False,
                "result": {"duplicate_count": dup_count, "sample_duplicate_values": dup_values}
            })

    # 4) type coercion checks for columns declared in config
    for col, desired_type in col_types.items():
        if col not in df.columns:
            continue
        check = _coerce_and_check_series(df[col], desired_type)
        if not check["success"]:
            failing.append({
                "expectation": "expect_column_type_convertible",
                "column": col,
                "success": False,
                "result": {"desired_type": desired_type, "failed_count": check["failed_count"], "sample_bad_values": check["sample_values"]}
            })

    # 5) STRICT format checks for Email, Date, Time
    # We run strict checks IF the column exists. Also allow overrides via validations_cfg.
    # validations_cfg can specify custom formats like: {"Date": "%d/%m/%Y", "Time": "%H:%M"}
    # For Email, specify "email".
    # For Date and Time, provide strptime format string.
    # If not provided, use defaults: Email -> strict regex, Date -> "%Y-%m-%d", Time -> "%H:%M:%S"

    # EMAIL
    if "Email" in df.columns:
        invalid_mask = df["Email"].isna() | df["Email"].str.strip().eq("") | ~df["Email"].apply(lambda v: validate_email_strict(v))
        invalid_count = int(invalid_mask.sum())
        if invalid_count > 0:
            failing.append({
                "expectation": "expect_column_values_to_match_strict_email",
                "column": "Email",
                "success": False,
                "result": {"invalid_count": invalid_count, "sample_invalid_values": df.loc[invalid_mask, "Email"].head(10).astype(str).tolist()}
            })

    # DATE
    if "Date" in df.columns:
        date_fmt = validations_cfg.get("Date", "%Y-%m-%d")
        # Use validate_date_strict with the given format
        invalid_mask = df["Date"].isna() | df["Date"].str.strip().eq("") | ~df["Date"].apply(lambda v: validate_date_strict(v, fmt=date_fmt))
        invalid_count = int(invalid_mask.sum())
        if invalid_count > 0:
            failing.append({
                "expectation": "expect_column_values_to_match_strict_date",
                "column": "Date",
                "success": False,
                "result": {"invalid_count": invalid_count, "sample_invalid_values": df.loc[invalid_mask, "Date"].head(10).astype(str).tolist(), "expected_format": date_fmt}
            })

    # TIME
    if "Time" in df.columns:
        time_fmt = validations_cfg.get("Time", "%H:%M:%S")
        invalid_mask = df["Time"].isna() | df["Time"].str.strip().eq("") | ~df["Time"].apply(lambda v: validate_time_strict(v, fmt=time_fmt))
        invalid_count = int(invalid_mask.sum())
        if invalid_count > 0:
            failing.append({
                "expectation": "expect_column_values_to_match_strict_time",
                "column": "Time",
                "success": False,
                "result": {"invalid_count": invalid_count, "sample_invalid_values": df.loc[invalid_mask, "Time"].head(10).astype(str).tolist(), "expected_format": time_fmt}
            })

    total_expectations = len(required) + len(unique_keys) + len(col_types) + 3  # +3 for strict checks (Email,Date,Time)
    unsuccessful = len(failing)
    successful = max(0, total_expectations - unsuccessful)

    result = {
        "success": unsuccessful == 0,
        "statistics": {
            "evaluated_expectations": total_expectations,
            "successful_expectations": successful,
            "unsuccessful_expectations": unsuccessful,
            "rows": total_rows
        },
        "results": failing,
        "meta": {
            "csv": str(csv_p),
            "config_path": str(CONFIG_PATH)
        }
    }

    if save_result:
        VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
        out_file = VALIDATION_DIR / f"{csv_p.stem}_result.json"
        with out_file.open("w", encoding="utf-8") as fo:
            json.dump(result, fo, indent=2, default=str)

    return result


def print_summary(result: dict):
    print("\nValidation summary:")
    print(f"  success: {result.get('success')}")
    stats = result.get("statistics", {}) or {}
    print(f"  evaluated_expectations: {stats.get('evaluated_expectations')}")
    print(f"  successful_expectations: {stats.get('successful_expectations')}")
    print(f"  unsuccessful_expectations: {stats.get('unsuccessful_expectations')}")
    print(f"  rows: {stats.get('rows')}")
    failing = result.get("results", []) or []
    if failing:
        print("\nFailing expectations (up to 10):")
        for f in failing[:10]:
            exp = f.get("expectation")
            col = f.get("column", "<table-level>")
            reason = f.get("reason") or f.get("result")
            print(f" - {exp} on {col}: {reason}")
    else:
        print("No failing expectations.")


def _cli():
    parser = argparse.ArgumentParser(description="Lightweight CSV validator (strict Email/Date/Time).")
    parser.add_argument("csv", help="Path to CSV file to validate")
    parser.add_argument("--no-save", action="store_true", help="Do not save JSON report")
    parser.add_argument("--raise-on-fail", action="store_true", help="Raise ValidationError (exit non-zero) when validation fails")
    args = parser.parse_args()

    try:
        res = validate_csv(args.csv, save_result=not args.no_save)
        print_summary(res)
        if (not res.get("success", False)) and args.raise_on_fail:
            raise ValidationError("Validation failed; use validation_results/ for details.")
        sys.exit(0 if res.get("success", False) else 2)
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(3)
    except ValidationError as e:
        print(f"VALIDATION ERROR: {e}")
        sys.exit(4)
    except Exception as e:
        print("UNEXPECTED ERROR:")
        traceback.print_exc()
        sys.exit(5)


if __name__ == "__main__":
    _cli()
