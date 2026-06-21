"""Delta 소스 어댑터 검증. deltalake 미설치면 모듈 전체 skip."""

from __future__ import annotations

import pyarrow as pa
import pytest

pytest.importorskip("deltalake")
from deltalake import DeltaTable, write_deltalake  # noqa: E402

from lakesift import DeltaSource, ParquetSource, diff  # noqa: E402


def _delta(tmp_path, name: str, data: pa.Table) -> str:
    """로컬 디렉터리에 Delta 테이블을 쓰고 경로(str) 반환."""
    path = tmp_path / name
    write_deltalake(str(path), data)
    return str(path)


def test_delta_cell_diff(tmp_path):
    left = _delta(tmp_path, "left", pa.table({"id": pa.array([1, 2, 3], pa.int64()), "v": ["a", "b", "c"]}))
    right = _delta(tmp_path, "right", pa.table({"id": pa.array([2, 3, 4], pa.int64()), "v": ["b", "C", "d"]}))
    with diff(DeltaSource(left), DeltaSource(right), key=["id"]) as r:
        assert [row["id"] for row in r.removed] == [1]
        assert [row["id"] for row in r.added] == [4]
        cells = list(r.changed_cells)
        assert len(cells) == 1 and cells[0].key == {"id": 3}
        assert cells[0].old == "c" and cells[0].new == "C"


def test_delta_identical_is_empty(tmp_path):
    data = pa.table({"id": pa.array([1, 2], pa.int64()), "v": ["x", "y"]})
    left = _delta(tmp_path, "a", data)
    right = _delta(tmp_path, "b", data)
    with diff(DeltaSource(left), DeltaSource(right), key=["id"]) as r:
        assert r.is_empty()


def test_delta_time_travel_version(tmp_path):
    """version 으로 이전 버전 타임트래블 → 같은 테이블의 v0 vs v1 비교."""
    path = tmp_path / "t"
    write_deltalake(str(path), pa.table({"id": pa.array([1, 2], pa.int64()), "v": ["a", "b"]}))
    write_deltalake(str(path), pa.table({"id": pa.array([1, 2], pa.int64()), "v": ["a", "B"]}), mode="overwrite")
    with diff(
        DeltaSource(str(path), version=0), DeltaSource(str(path), version=1), key=["id"]
    ) as r:
        cells = list(r.changed_cells)
        assert len(cells) == 1 and cells[0].old == "b" and cells[0].new == "B"


def test_delta_arrow_schema_is_metadata_only(tmp_path):
    path = _delta(tmp_path, "s", pa.table({"id": pa.array([1], pa.int64()), "v": ["a"], "w": ["b"]}))
    assert set(DeltaSource(path).arrow_schema().names) == {"id", "v", "w"}


def test_delta_to_relation_projection(tmp_path):
    import duckdb

    path = _delta(tmp_path, "p", pa.table({"id": pa.array([1], pa.int64()), "v": ["a"], "w": ["b"]}))
    con = duckdb.connect()
    assert DeltaSource(path).to_relation(con, columns=["id", "w"]).columns == ["id", "w"]


def test_delta_columns_pushdown_projects_rows(tmp_path):
    """--columns 로 비교하면 added 행도 key+비교대상만 보인다(pushdown)."""
    left = _delta(tmp_path, "pl", pa.table({"id": pa.array([1], pa.int64()), "v": ["a"], "w": ["x"]}))
    right = _delta(tmp_path, "pr", pa.table({"id": pa.array([1, 2], pa.int64()), "v": ["a", "b"], "w": ["x", "y"]}))
    with diff(DeltaSource(left), DeltaSource(right), key=["id"], columns=["v"]) as r:
        added = list(r.added)
    assert added and set(added[0].keys()) == {"id", "v"}


def test_delta_vs_parquet(tmp_path):
    """소스 혼합: 왼쪽 delta, 오른쪽 parquet 도 동일 코어로 비교된다."""
    import pyarrow.parquet as pq

    left = _delta(tmp_path, "delta_t", pa.table({"id": pa.array([1, 2], pa.int64()), "v": ["a", "b"]}))
    ppath = tmp_path / "r.parquet"
    pq.write_table(pa.table({"id": pa.array([1, 2], pa.int64()), "v": ["a", "B"]}), ppath)
    with diff(DeltaSource(left), ParquetSource(str(ppath)), key=["id"]) as r:
        assert r.summary()["changed_cells"] == 1


def test_delta_accepts_table_instance(tmp_path):
    """경로 대신 이미 로드한 DeltaTable 인스턴스도 받는다."""
    path = tmp_path / "inst"
    write_deltalake(str(path), pa.table({"id": pa.array([1], pa.int64()), "v": ["a"]}))
    dt = DeltaTable(str(path))
    with diff(DeltaSource(dt), DeltaSource(dt), key=["id"]) as r:
        assert r.is_empty()
