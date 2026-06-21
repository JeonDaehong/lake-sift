"""Column projection pushdown — verify the core only asks each source for key + compared.

Projection is enabled only when --columns/--exclude is given. When enabled, added/removed
rows show only the requested columns, while schema-change detection still uses the full
schema (read separately from the scan).
"""

from __future__ import annotations

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from lakesift import ParquetSource, diff


class RecordingSource:
    """A fake source that records which columns to_relation was asked for."""

    def __init__(self, table: pa.Table):
        self.table = table
        self.calls: list[list[str] | None] = []

    def arrow_schema(self) -> pa.Schema:
        return self.table.schema

    def to_relation(self, con, *, columns=None):
        self.calls.append(list(columns) if columns is not None else None)
        t = self.table if columns is None else self.table.select(list(columns))
        return con.from_arrow(t)


def _t(**cols) -> pa.Table:
    return pa.table(cols)


def test_columns_pushes_down_key_plus_compared():
    left = RecordingSource(_t(id=[1, 2], a=["x", "y"], b=["p", "q"], c=["m", "n"]))
    right = RecordingSource(_t(id=[1, 2], a=["x", "Y"], b=["p", "q"], c=["m", "n"]))
    with diff(left, right, key=["id"], columns=["a"]) as r:
        assert r.summary()["changed_cells"] == 1
    # projection active: schema via arrow_schema, to_relation called once with key+compare.
    assert left.calls == [["id", "a"]]
    assert right.calls == [["id", "a"]]


def test_exclude_pushes_down_remaining_columns():
    left = RecordingSource(_t(id=[1], a=["x"], b=["p"], c=["m"]))
    right = RecordingSource(_t(id=[1], a=["x"], b=["P"], c=["m"]))
    with diff(left, right, key=["id"], exclude=["b", "c"]) as r:
        # b is excluded so its change isn't detected -> identical
        assert r.is_empty()
    assert left.calls == [["id", "a"]]


def test_no_projection_reads_full_columns():
    left = RecordingSource(_t(id=[1, 2], a=["x", "y"], b=["p", "q"]))
    right = RecordingSource(_t(id=[1, 2], a=["x", "y"], b=["p", "q"]))
    with diff(left, right, key=["id"]) as r:
        assert r.is_empty()
    # inactive: a single full scan, no projection (None).
    assert left.calls == [None]
    assert right.calls == [None]


def test_schema_changes_still_detected_under_projection():
    # b only on left, d only on right. Even with --columns a, b/d are reported as schema changes.
    left = RecordingSource(_t(id=[1], a=["x"], b=["only-left"]))
    right = RecordingSource(_t(id=[1], a=["x"], d=["only-right"]))
    with diff(left, right, key=["id"], columns=["a"]) as r:
        kinds = {(sc.column, sc.kind) for sc in r.schema_changes}
    assert ("b", "removed") in kinds
    assert ("d", "added") in kinds
    # only the projected columns were read (non-existent b/d were never requested).
    assert left.calls == [["id", "a"]]
    assert right.calls == [["id", "a"]]


def test_parquet_projection_limits_added_row_columns(tmp_path):
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"
    pq.write_table(_t(id=[1, 2], name=["a", "b"], extra=["p", "q"]), a)
    pq.write_table(_t(id=[1, 2, 3], name=["a", "b", "c"], extra=["p", "q", "r"]), b)
    with diff(ParquetSource(str(a)), ParquetSource(str(b)), key=["id"], columns=["name"]) as r:
        added = list(r.added)
    # the added row (id=3) has only the projected columns (no extra).
    assert added and set(added[0].keys()) == {"id", "name"}


def test_parquet_source_to_relation_projects(tmp_path):
    path = tmp_path / "t.parquet"
    pq.write_table(_t(id=[1], a=["x"], b=["y"]), path)
    con = duckdb.connect()
    src = ParquetSource(str(path))
    assert src.to_relation(con).columns == ["id", "a", "b"]
    assert src.to_relation(con, columns=["id", "a"]).columns == ["id", "a"]
