from pathlib import Path
import shutil
import sys
import os

def test_validation_sample(tmp_path, monkeypatch):
    # copy sample data to tmp staged location and run validate
    repo_root = Path(__file__).resolve().parents[1]
    sample = repo_root / "sample_data" / "sample_transactions.csv"
    assert sample.exists()

    staged = tmp_path / "staged.csv"
    shutil.copy2(sample, staged)

    # run validate
    sys.path.insert(0, str(repo_root))
    from etl import validate
    ok = validate.run_validation(str(staged))
    assert ok is True
