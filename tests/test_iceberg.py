"""Iceberg source adapter. The whole module is skipped if pyiceberg isn't installed."""

from __future__ import annotations

import os

import pyarrow as pa
import pytest

pytest.importorskip("pyiceberg")
from pyiceberg.catalog.sql import SqlCatalog  # noqa: E402

from lakesift import IcebergSource, ParquetSource, diff  # noqa: E402


def _ice(tmp_path, name: str, data: pa.Table):
    """Create one iceberg table in a local SQL catalog, load data, return the Table."""
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
    """With --columns, added rows show only key + compared columns (pushdown)."""
    left = _ice(tmp_path, "pl", pa.table({"id": pa.array([1], pa.int64()), "v": ["a"], "w": ["x"]}))
    right = _ice(tmp_path, "pr", pa.table({"id": pa.array([1, 2], pa.int64()), "v": ["a", "b"], "w": ["x", "y"]}))
    with diff(IcebergSource(left), IcebergSource(right), key=["id"], columns=["v"]) as r:
        added = list(r.added)
    assert added and set(added[0].keys()) == {"id", "v"}


def test_iceberg_branch_diff_wap(tmp_path):
    """Write-Audit-Publish: diff a staging branch against main before merging."""
    t = _ice(tmp_path, "wap", pa.table({"id": pa.array([1, 2, 3], pa.int64()), "status": ["paid", "pending", "paid"]}))
    main_snap = t.current_snapshot().snapshot_id
    t.manage_snapshots().create_branch(main_snap, "staging").commit()
    t.refresh()
    # the audited write lands only on the staging branch
    t.append(pa.table({"id": pa.array([4], pa.int64()), "status": ["new"]}), branch="staging")
    t.refresh()

    main = IcebergSource(t, ref="main")
    staging = IcebergSource(t, ref="staging")
    with diff(main, staging, key=["id"]) as r:
        # staging adds order 4 and nothing else changed
        assert [row["id"] for row in r.added] == [4]
        assert list(r.removed) == []
        assert r.summary()["changed_cells"] == 0


def test_iceberg_vs_parquet(tmp_path):
    """Mixed sources: left iceberg, right parquet are compared by the same core."""
    import pyarrow.parquet as pq

    left = _ice(tmp_path, "ice", pa.table({"id": pa.array([1, 2], pa.int64()), "v": ["a", "b"]}))
    ppath = tmp_path / "r.parquet"
    pq.write_table(pa.table({"id": pa.array([1, 2], pa.int64()), "v": ["a", "B"]}), ppath)
    with diff(IcebergSource(left), ParquetSource(str(ppath)), key=["id"]) as r:
        assert r.summary()["changed_cells"] == 1
