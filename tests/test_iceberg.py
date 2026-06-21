"""Iceberg 소스 어댑터 검증. pyiceberg 미설치면 모듈 전체 skip."""

from __future__ import annotations

import os

import pyarrow as pa
import pytest

pytest.importorskip("pyiceberg")
from pyiceberg.catalog.sql import SqlCatalog  # noqa: E402

from lakesift import IcebergSource, ParquetSource, diff  # noqa: E402


def _ice(tmp_path, name: str, data: pa.Table):
    """로컬 SQL 카탈로그에 iceberg 테이블 하나 만들고 데이터 적재 후 Table 반환."""
    wh = tmp_path / f"wh_{name}"
    os.makedirs(wh, exist_ok=True)
    cat = SqlCatalog(
        name,
        uri=f"sqlite:///{tmp_path / (name + '.db')}",
        warehouse="file://" + str(wh).replace("\\", "/"),
    )
    cat.create_namespace("ns")
    t = cat.create_table(f"ns.{name}", schema=data.schema)
    t.append(data)
    return t


def test_iceberg_cell_diff(tmp_path):
    left = _ice(tmp_path, "left", pa.table({"id": pa.array([1, 2, 3], pa.int64()), "v": ["a", "b", "c"]}))
    right = _ice(tmp_path, "right", pa.table({"id": pa.array([2, 3, 4], pa.int64()), "v": ["b", "C", "d"]}))
    with diff(IcebergSource(left), IcebergSource(right), key=["id"]) as r:
        assert [row["id"] for row in r.removed] == [1]
        assert [row["id"] for row in r.added] == [4]
        cells = list(r.changed_cells)
        assert len(cells) == 1 and cells[0].key == {"id": 3}
        assert cells[0].old == "c" and cells[0].new == "C"


def test_iceberg_identical_is_empty(tmp_path):
    data = pa.table({"id": pa.array([1, 2], pa.int64()), "v": ["x", "y"]})
    left = _ice(tmp_path, "a", data)
    right = _ice(tmp_path, "b", data)
    with diff(IcebergSource(left), IcebergSource(right), key=["id"]) as r:
        assert r.is_empty()


def test_iceberg_arrow_schema_is_metadata_only(tmp_path):
    t = _ice(tmp_path, "s", pa.table({"id": pa.array([1], pa.int64()), "v": ["a"], "w": ["b"]}))
    assert set(IcebergSource(t).arrow_schema().names) == {"id", "v", "w"}


def test_iceberg_to_relation_projection(tmp_path):
    import duckdb

    t = _ice(tmp_path, "p", pa.table({"id": pa.array([1], pa.int64()), "v": ["a"], "w": ["b"]}))
    con = duckdb.connect()
    assert IcebergSource(t).to_relation(con, columns=["id", "w"]).columns == ["id", "w"]


def test_iceberg_columns_pushdown_projects_rows(tmp_path):
    """--columns 로 비교하면 added 행도 key+비교대상만 보인다(pushdown)."""
    left = _ice(tmp_path, "pl", pa.table({"id": pa.array([1], pa.int64()), "v": ["a"], "w": ["x"]}))
    right = _ice(tmp_path, "pr", pa.table({"id": pa.array([1, 2], pa.int64()), "v": ["a", "b"], "w": ["x", "y"]}))
    with diff(IcebergSource(left), IcebergSource(right), key=["id"], columns=["v"]) as r:
        added = list(r.added)
    assert added and set(added[0].keys()) == {"id", "v"}


def test_iceberg_vs_parquet(tmp_path):
    """소스 혼합: 왼쪽 iceberg, 오른쪽 parquet 도 동일 코어로 비교된다."""
    import pyarrow.parquet as pq

    left = _ice(tmp_path, "ice", pa.table({"id": pa.array([1, 2], pa.int64()), "v": ["a", "b"]}))
    ppath = tmp_path / "r.parquet"
    pq.write_table(pa.table({"id": pa.array([1, 2], pa.int64()), "v": ["a", "B"]}), ppath)
    with diff(IcebergSource(left), ParquetSource(str(ppath)), key=["id"]) as r:
        assert r.summary()["changed_cells"] == 1
