"""컬럼 projection pushdown — 코어가 key+비교대상만 소스에 요청하는지 검증.

projection 은 --columns/--exclude 가 주어질 때만 활성. 활성 시 added/removed 행도
요청한 컬럼만 보이고, 스키마 변경 감지는 (스캔과 분리된) 전체 스키마 기준으로 유지된다.
"""

from __future__ import annotations

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from lakesift import ParquetSource, diff


class RecordingSource:
    """to_relation 이 어떤 컬럼을 요청받았는지 기록하는 가짜 소스."""

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
    # projection 활성: 스키마는 arrow_schema 로 보고, to_relation 은 key+compare 만 1회.
    assert left.calls == [["id", "a"]]
    assert right.calls == [["id", "a"]]


def test_exclude_pushes_down_remaining_columns():
    left = RecordingSource(_t(id=[1], a=["x"], b=["p"], c=["m"]))
    right = RecordingSource(_t(id=[1], a=["x"], b=["P"], c=["m"]))
    with diff(left, right, key=["id"], exclude=["b", "c"]) as r:
        # b 는 제외돼 변경으로 안 잡힘 → 동일
        assert r.is_empty()
    assert left.calls == [["id", "a"]]


def test_no_projection_reads_full_columns():
    left = RecordingSource(_t(id=[1, 2], a=["x", "y"], b=["p", "q"]))
    right = RecordingSource(_t(id=[1, 2], a=["x", "y"], b=["p", "q"]))
    with diff(left, right, key=["id"]) as r:
        assert r.is_empty()
    # 비활성: 전체 스캔 한 번, projection 없음(None).
    assert left.calls == [None]
    assert right.calls == [None]


def test_schema_changes_still_detected_under_projection():
    # left 에만 b, right 에만 d. --columns a 로 a 만 비교해도 b/d 는 스키마 변경으로 보고.
    left = RecordingSource(_t(id=[1], a=["x"], b=["only-left"]))
    right = RecordingSource(_t(id=[1], a=["x"], d=["only-right"]))
    with diff(left, right, key=["id"], columns=["a"]) as r:
        kinds = {(sc.column, sc.kind) for sc in r.schema_changes}
    assert ("b", "removed") in kinds
    assert ("d", "added") in kinds
    # 데이터는 projection 된 컬럼만 읽혔다(존재하지 않는 b/d 를 요청하지 않음).
    assert left.calls == [["id", "a"]]
    assert right.calls == [["id", "a"]]


def test_parquet_projection_limits_added_row_columns(tmp_path):
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"
    pq.write_table(_t(id=[1, 2], name=["a", "b"], extra=["p", "q"]), a)
    pq.write_table(_t(id=[1, 2, 3], name=["a", "b", "c"], extra=["p", "q", "r"]), b)
    with diff(ParquetSource(str(a)), ParquetSource(str(b)), key=["id"], columns=["name"]) as r:
        added = list(r.added)
    # added 행(id=3)은 projection 된 컬럼만 가진다 (extra 없음).
    assert added and set(added[0].keys()) == {"id", "name"}


def test_parquet_source_to_relation_projects(tmp_path):
    path = tmp_path / "t.parquet"
    pq.write_table(_t(id=[1], a=["x"], b=["y"]), path)
    con = duckdb.connect()
    src = ParquetSource(str(path))
    assert src.to_relation(con).columns == ["id", "a", "b"]
    assert src.to_relation(con, columns=["id", "a"]).columns == ["id", "a"]
