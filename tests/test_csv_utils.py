from pathlib import Path

import pytest

from ingestion_service.csv_utils import _load_csv


def _write(tmp_path, name: str, content: str) -> Path:
    p = tmp_path / name
    p.write_text(content, encoding="utf-8")
    return p


def test_load_csv_happy_path(tmp_path):
    csv_text = "id,name\n1,Alice\n2,Bob\n"
    path = _write(tmp_path, "simple.csv", csv_text)
    rows = list(_load_csv(path))
    assert rows == [
        {"id": "1", "name": "Alice"},
        {"id": "2", "name": "Bob"},
    ]


def test_load_csv_cleans_whitespace(tmp_path):
    csv_text = "id , name \n 1 , Alice \n"
    path = _write(tmp_path, "ws.csv", csv_text)
    rows = list(_load_csv(path))
    # Values are cleaned; header whitespace is preserved by csv module
    assert list(rows[0].values()) == ["1", "Alice"]


def test_load_csv_extra_cells_raises(tmp_path):
    csv_text = "id,name\n1,Alice,extra\n"
    path = _write(tmp_path, "bad.csv", csv_text)
    with pytest.raises(ValueError):
        list(_load_csv(path)) 