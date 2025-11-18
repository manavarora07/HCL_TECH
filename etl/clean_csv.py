#!/usr/bin/env python3
"""
etl/clean_csv.py

Usage (from repo root):
  # preview cleaning (doesn't write unless --out)
  python etl/clean_csv.py --csv sample_data/retail_data_Source.csv --preview

  # write cleaned CSV
  python etl/clean_csv.py --csv sample_data/retail_data_Source.csv --out data/cleaned.csv --fill-defaults

Options:
  --csv PATH           input CSV (required)
  --out PATH           output cleaned CSV (defaults to data/cleaned.csv)
  --fill-defaults      fill missing Transaction_ID/Customer_ID/Email with defaults
  --drop-missing       drop rows missing critical fields (Amount or Email)
  --preview            only show counts and sample fixes, do not write file
  --date-formats       comma-separated list of input date formats to try (default: %m/%d/%Y,%Y-%m-%d)
  --time-formats       comma-separated list of input time formats to try (default: %H:%M:%S,%H:%M)
"""
from pathlib import Path
import argparse
import pandas as pd
import uuid
from datetime import datetime

DEFAULT_OUT = Path("data/cleaned.csv")


def try_parse_date(val, fmts):
    if pd.isna(val): 
        return None
    s = str(val).strip()
    if s == "":
        return None
    for f in fmts:
        try:
            dt = datetime.strptime(s, f)
            return dt.strftime("%Y-%m-%d")  # normalized target
        except Exception:
            continue
    return None


def try_parse_time(val, fmts):
    if pd.isna(val):
        return None
    s = str(val).strip()
    if s == "":
        return None
    for f in fmts:
        try:
            dt = datetime.strptime(s, f)
            return dt.strftime("%H:%M:%S")  # normalized target
        except Exception:
            continue
    return None


def clean_csv(in_path: Path, out_path: Path = DEFAULT_OUT, fill_defaults: bool = False, drop_missing: bool = False, date_formats=None, time_formats=None, preview=False):
    date_formats = date_formats or ["%m/%d/%Y", "%Y-%m-%d"]
    time_formats = time_formats or ["%H:%M:%S", "%H:%M"]

    df = pd.read_csv(in_path, dtype=str)

    # normalize whitespace
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # 1) normalize Date -> YYYY-MM-DD
    df["__parsed_date"] = df["Date"].apply(lambda v: try_parse_date(v, date_formats) if "Date" in df.columns else None)

    # 2) normalize Time -> HH:MM:SS
    if "Time" in df.columns:
        df["__parsed_time"] = df["Time"].apply(lambda v: try_parse_time(v, time_formats))
    else:
        df["__parsed_time"] = None

    # 3) fill defaults if requested
    if fill_defaults:
        # Transaction_ID
        if "Transaction_ID" in df.columns:
            missing_tx = df["Transaction_ID"].isna() | (df["Transaction_ID"].astype(str).str.strip() == "") | (df["Transaction_ID"].astype(str).str.lower() == "nan")
            df.loc[missing_tx, "Transaction_ID"] = [str(uuid.uuid4()) for _ in range(missing_tx.sum())]

        # Customer_ID
        if "Customer_ID" in df.columns:
            missing_cust = df["Customer_ID"].isna() | (df["Customer_ID"].astype(str).str.strip() == "") | (df["Customer_ID"].astype(str).str.lower() == "nan")
            df.loc[missing_cust, "Customer_ID"] = "unknown"

        # Email
        if "Email" in df.columns:
            missing_email = df["Email"].isna() | (df["Email"].astype(str).str.strip() == "") | (df["Email"].astype(str).str.lower() == "nan")
            df.loc[missing_email, "Email"] = "unknown@example.com"

    # 4) drop rows with missing critical fields if requested (Amount or Email)
    if drop_missing:
        cond_amount_missing = ("Amount" in df.columns) and (df["Amount"].isna() | (df["Amount"].astype(str).str.strip() == "") | (df["Amount"].astype(str).str.lower() == "nan"))
        cond_email_missing = ("Email" in df.columns) and (df["Email"].isna() | (df["Email"].astype(str).str.strip() == "") | (df["Email"].astype(str).str.lower() == "nan"))
        # keep rows where both are present
        keep_mask = ~ (cond_amount_missing | cond_email_missing)
        dropped = (~keep_mask).sum()
        df = df[keep_mask]
    else:
        dropped = 0

    # 5) apply parsed normalized date/time back to columns
    if "__parsed_date" in df.columns:
        # where parsed exists, overwrite Date with normalized; else keep original
        df.loc[df["__parsed_date"].notna(), "Date"] = df.loc[df["__parsed_date"].notna(), "__parsed_date"]
        df = df.drop(columns="__parsed_date")
    if "__parsed_time" in df.columns:
        df.loc[df["__parsed_time"].notna(), "Time"] = df.loc[df["__parsed_time"].notna(), "__parsed_time"]
        df = df.drop(columns="__parsed_time")

    # 6) optional: coerce numeric columns (Item_Price, Amount) to numeric types and fill empties with NaN
    for col in ["Item_Price", "Amount", "Total_Amount", "Total_Purchases", "Ratings"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Summary
    summary = {
        "original_rows": None,
        "final_rows": int(df.shape[0]),
        "dropped_rows": int(dropped),
        "missing_transaction_ids_after": int((df["Transaction_ID"].isna() | (df["Transaction_ID"].astype(str).str.strip() == "")).sum()) if "Transaction_ID" in df.columns else 0,
        "missing_customer_ids_after": int((df["Customer_ID"].isna() | (df["Customer_ID"].astype(str).str.strip() == "")).sum()) if "Customer_ID" in df.columns else 0,
        "missing_emails_after": int((df["Email"].isna() | (df["Email"].astype(str).str.strip() == "")).sum()) if "Email" in df.columns else 0,
    }

    if preview:
        print("Preview summary (no file written):")
        print(summary)
        print("Sample rows (first 5):")
        print(df.head(5).to_dict(orient="records"))
        return summary

    # Ensure output folder exists
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"Wrote cleaned file to: {out_path}")
    print("Summary:", summary)
    return summary


def _cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    parser.add_argument("--fill-defaults", action="store_true")
    parser.add_argument("--drop-missing", action="store_true")
    parser.add_argument("--preview", action="store_true")
    parser.add_argument("--date-formats", default="%m/%d/%Y,%Y-%m-%d")
    parser.add_argument("--time-formats", default="%H:%M:%S,%H:%M")
    args = parser.parse_args()

    date_formats = [s.strip() for s in args.date_formats.split(",") if s.strip()]
    time_formats = [s.strip() for s in args.time_formats.split(",") if s.strip()]

    clean_csv(
        Path(args.csv),
        out_path=Path(args.out),
        fill_defaults=args.fill_defaults,
        drop_missing=args.drop_missing,
        date_formats=date_formats,
        time_formats=time_formats,
        preview=args.preview
    )


if __name__ == "__main__":
    _cli()
