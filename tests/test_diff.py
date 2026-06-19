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
    with diff(ParquetSource(a), ParquetSource(b), key=["id"]) as result:
        assert [r["id"] for r in result.removed] == [1]      # left only
        assert [r["id"] for r in result.added] == [4]        # right only
        assert result.changed_rows == 1                       # id=3 의 v: c→C
        cells = list(result.changed_cells)
        assert len(cells) == 1
        cc = cells[0]
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
    with diff(ParquetSource(a), ParquetSource(b), key=["o", "l"]) as result:
        assert result.changed_rows == 1
        assert next(result.changed_cells).key == {"o": 1, "l": 2}


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


def test_changed_by_column_ranking(tmp_path):
    # a 컬럼은 3건, b 컬럼은 1건 변경 → 내림차순 [a, b]
    a = _write(tmp_path / "a.parquet", {"id": [1, 2, 3], "a": ["x", "x", "x"], "b": ["p", "q", "r"]})
    b = _write(tmp_path / "b.parquet", {"id": [1, 2, 3], "a": ["X", "Y", "Z"], "b": ["p", "q", "R"]})
    with diff(ParquetSource(a), ParquetSource(b), key=["id"]) as r:
        assert r.changed_by_column == [("a", 3), ("b", 1)]
        # 안 바뀐 컬럼은 빠지고, 총합은 changed_cells 와 일치
        assert sum(n for _, n in r.changed_by_column) == r.summary()["changed_cells"]


def test_tolerance_numeric(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1, 2], "v": [1.00, 5.0]})
    b = _write(tmp_path / "b.parquet", {"id": [1, 2], "v": [1.05, 9.0]})
    # tol 0.1: id=1 (0.05 차) 은 같음, id=2 (4.0 차) 만 변경
    with diff(ParquetSource(a), ParquetSource(b), key=["id"], tolerance=0.1) as r:
        assert r.summary()["changed_cells"] == 1
        assert next(r.changed_cells).key == {"id": 2}
    # tol 없으면 둘 다 변경
    with diff(ParquetSource(a), ParquetSource(b), key=["id"]) as r:
        assert r.summary()["changed_cells"] == 2


def test_tolerance_ignores_string_columns(tmp_path):
    # tolerance 는 수치 컬럼에만 — 문자열은 그대로 정확 비교
    a = _write(tmp_path / "a.parquet", {"id": [1], "v": ["a"]})
    b = _write(tmp_path / "b.parquet", {"id": [1], "v": ["b"]})
    with diff(ParquetSource(a), ParquetSource(b), key=["id"], tolerance=10.0) as r:
        assert r.summary()["changed_cells"] == 1


def test_ignore_case(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1, 2], "v": ["Hello", "x"]})
    b = _write(tmp_path / "b.parquet", {"id": [1, 2], "v": ["hello", "y"]})
    with diff(ParquetSource(a), ParquetSource(b), key=["id"], ignore_case=True) as r:
        # id=1 은 대소문자만 다름 → 같음, id=2 만 변경
        assert r.summary()["changed_cells"] == 1
        assert next(r.changed_cells).key == {"id": 2}


def test_missing_key_errors(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1], "v": ["a"]})
    b = _write(tmp_path / "b.parquet", {"id": [1], "v": ["a"]})
    with pytest.raises(DiffError):
        diff(ParquetSource(a), ParquetSource(b), key=["nope"])
    with pytest.raises(DiffError):
        diff(ParquetSource(a), ParquetSource(b), key=[])
