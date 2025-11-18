import tempfile
from pathlib import Path
from etl.ingest import copy_to_dest

def test_copy_to_dest(tmp_path):
    src = tmp_path / "in.csv"
    src.write_text("a,b\n1,2\n")
    dest = tmp_path / "out.csv"
    out = copy_to_dest(str(src), dest)
    assert out.exists()
    assert out.read_text() == src.read_text()
