"""diff 코어 동작 검증. 픽스처 parquet 는 pyarrow 로 즉석 생성."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lakesift import ParquetSource, diff
from lakesift.core import DiffError


def _write(path, rows: dict):
    pq.write_table(pa.table(rows), path)
    return str(path)


def test_identical_is_empty(tmp_path):
    data = {"id": [1, 2, 3], "v": ["a", "b", "c"]}
    a = _write(tmp_path / "a.parquet", data)
    b = _write(tmp_path / "b.parquet", data)
    result = diff(ParquetSource(a), ParquetSource(b), key=["id"])
    assert result.is_empty()
    assert result.summary() == {
        "added": 0,
        "removed": 0,
        "changed": 0,
        "changed_cells": 0,
        "schema_changes": 0,
    }


def test_added_removed_changed(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1, 2, 3], "v": ["a", "b", "c"]})
    b = _write(tmp_path / "b.parquet", {"id": [2, 3, 4], "v": ["b", "C", "d"]})
    result = diff(ParquetSource(a), ParquetSource(b), key=["id"])

    assert [r["id"] for r in result.removed] == [1]      # left only
    assert [r["id"] for r in result.added] == [4]        # right only
    assert result.changed_rows == 1                       # id=3 의 v: c→C
    assert len(result.changed_cells) == 1
    cc = result.changed_cells[0]
    assert cc.key == {"id": 3} and cc.column == "v"
    assert cc.old == "c" and cc.new == "C"
    assert not result.is_empty()


def test_null_equals_null(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1, 2], "v": [None, "x"]})
    b = _write(tmp_path / "b.parquet", {"id": [1, 2], "v": [None, "x"]})
    result = diff(ParquetSource(a), ParquetSource(b), key=["id"])
    assert result.is_empty()  # NULL == NULL → 같음


def test_composite_key(tmp_path):
    a = _write(tmp_path / "a.parquet", {"o": [1, 1], "l": [1, 2], "v": ["a", "b"]})
    b = _write(tmp_path / "b.parquet", {"o": [1, 1], "l": [1, 2], "v": ["a", "B"]})
    result = diff(ParquetSource(a), ParquetSource(b), key=["o", "l"])
    assert result.changed_rows == 1
    assert result.changed_cells[0].key == {"o": 1, "l": 2}


def test_exclude_column(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1], "v": ["a"], "updated_at": ["t1"]})
    b = _write(tmp_path / "b.parquet", {"id": [1], "v": ["a"], "updated_at": ["t2"]})
    # updated_at 만 바뀜 → 제외하면 동일
    assert diff(ParquetSource(a), ParquetSource(b), key=["id"], exclude=["updated_at"]).is_empty()
    # 제외 안 하면 변경 감지
    assert not diff(ParquetSource(a), ParquetSource(b), key=["id"]).is_empty()


def test_schema_change(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1], "v": ["a"]})
    b = _write(tmp_path / "b.parquet", {"id": [1], "w": ["a"]})
    result = diff(ParquetSource(a), ParquetSource(b), key=["id"])
    kinds = {(c.column, c.kind) for c in result.schema_changes}
    assert ("v", "removed") in kinds
    assert ("w", "added") in kinds


def test_duplicate_key_errors(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1, 1], "v": ["a", "b"]})
    b = _write(tmp_path / "b.parquet", {"id": [1, 1], "v": ["a", "b"]})
    with pytest.raises(DiffError):
        diff(ParquetSource(a), ParquetSource(b), key=["id"])


def test_missing_key_errors(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1], "v": ["a"]})
    b = _write(tmp_path / "b.parquet", {"id": [1], "v": ["a"]})
    with pytest.raises(DiffError):
        diff(ParquetSource(a), ParquetSource(b), key=["nope"])
    with pytest.raises(DiffError):
        diff(ParquetSource(a), ParquetSource(b), key=[])
