#!/usr/bin/env python3
"""
ingest.py - small CLI for ingesting CSV files into data/ and optionally validating.
Usage:
    python etl/ingest.py path/to/file.csv [--dest data/staged.csv] [--validate]
"""
import argparse
from pathlib import Path
import shutil
import sys
import os

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DEST = ROOT / "data" / "staged.csv"

def copy_to_dest(src: str, dest: Path = DEFAULT_DEST) -> Path:
    src_p = Path(src).expanduser().resolve()
    if not src_p.exists():
        raise FileNotFoundError(f"Source not found: {src_p}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(str(src_p), str(dest))
    return dest

def run_validation_if_requested(dest: Path, do_validate: bool):
    if not do_validate:
        return True
    # attempt to import local validate module
    sys.path.insert(0, str(ROOT))
    try:
        from etl import validate
        print("Running validation...")
        return validate.run_validation(str(dest))
    except Exception as e:
        print(f"Validation failed: {e}")
        return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("src", help="Path to CSV to ingest")
    parser.add_argument("--dest", default=str(DEFAULT_DEST), help="Destination staged CSV")
    parser.add_argument("--validate", action="store_true", help="Run validation after copy")
    args = parser.parse_args()

    dest = Path(args.dest)
    try:
        out = copy_to_dest(args.src, dest)
        print(f"Copied {args.src} -> {out}")
    except Exception as e:
        print(f"Error copying file: {e}")
        raise SystemExit(1)

    ok = run_validation_if_requested(dest, args.validate)
    if not ok:
        print("Ingest completed with validation errors.")
        raise SystemExit(2)
    print("Ingest finished successfully.")

if __name__ == "__main__":
    main()
