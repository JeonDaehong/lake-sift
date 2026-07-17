"""audit() — before/after diff of a job's effect on an Iceberg table.

Skipped entirely when pyiceberg isn't installed.
"""

from __future__ import annotations

import os

import pyarrow as pa
import pytest

pytest.importorskip("pyiceberg")
from pyiceberg.catalog.sql import SqlCatalog  # noqa: E402

from lakesift import audit  # noqa: E402
from lakesift.core import DiffError  # noqa: E402


def _table(tmp_path, name: str, data: pa.Table | None = None):
    """Create an Iceberg table in a local SQL catalog; optionally seed it with `data`."""
    wh = tmp_path / f"wh_{name}"
    os.makedirs(wh, exist_ok=True)
    cat = SqlCatalog(
        name,
        uri=f"sqlite:///{tmp_path / (name + '.db')}",
        warehouse="file://" + str(wh).replace("\\", "/"),
    )
    cat.create_namespace("ns")
    schema = (data or pa.table({"id": pa.array([], pa.int64()), "v": pa.array([], pa.string())})).schema
    t = cat.create_table(f"ns.{name}", schema=schema)
    if data is not None:
        t.append(data)
    return t


def test_audit_reports_what_the_job_did(tmp_path):
    """The block appends and rewrites rows; audit isolates exactly those changes."""
    t = _table(tmp_path, "job", pa.table({"id": pa.array([1, 2, 3], pa.int64()), "v": ["a", "b", "c"]}))

    with audit(t, key=["id"]) as a:
        # the "job": add id=4, change id=2's value (overwrite = delete+append, 2 snapshots)
        t.overwrite(
            pa.table({"id": pa.array([1, 2, 3, 4], pa.int64()), "v": ["a", "B", "c", "d"]})
        )

    r = a.result
    assert [row["id"] for row in r.added] == [4]
    assert list(r.removed) == []
    cells = list(r.changed_cells)
    assert len(cells) == 1 and cells[0].key == {"id": 2}
    assert cells[0].old == "b" and cells[0].new == "B"
    assert a.before_snapshot_id is not None and a.after_snapshot_id != a.before_snapshot_id
    a.close()


def test_audit_noop_job_is_empty(tmp_path):
    """A job that commits nothing leaves before == after: an empty diff."""
    t = _table(tmp_path, "noop", pa.table({"id": pa.array([1, 2], pa.int64()), "v": ["x", "y"]}))

    with audit(t, key=["id"]) as a:
        pass  # no commit

    assert a.result.is_empty()
    assert a.after_snapshot_id == a.before_snapshot_id
    a.close()


def test_audit_first_load_from_empty_table(tmp_path):
    """When the table starts empty, every row the job writes is an addition."""
    t = _table(tmp_path, "fresh")  # created, no data -> no snapshot

    with audit(t, key=["id"]) as a:
        t.append(pa.table({"id": pa.array([1, 2], pa.int64()), "v": ["a", "b"]}))

    r = a.result
    assert a.before_snapshot_id is None
    assert sorted(row["id"] for row in r.added) == [1, 2]
    assert list(r.removed) == []
    assert r.summary()["changed_cells"] == 0
    a.close()


def test_audit_propagates_block_exception_without_diffing(tmp_path):
    """A failed job has no meaningful 'after' — the error propagates, no result is set."""
    t = _table(tmp_path, "fail", pa.table({"id": pa.array([1], pa.int64()), "v": ["a"]}))

    with pytest.raises(ValueError, match="boom"):
        with audit(t, key=["id"]) as a:
            raise ValueError("boom")

    assert a.result is None


def test_audit_forwards_diff_options(tmp_path):
    """Extra kwargs reach diff(): tolerance here suppresses a tiny numeric change."""
    t = _table(tmp_path, "opts", pa.table({"id": pa.array([1, 2], pa.int64()), "v": pa.array([1.0, 2.0], pa.float64())}))

    with audit(t, key=["id"], tolerance=0.5) as a:
        t.overwrite(pa.table({"id": pa.array([1, 2], pa.int64()), "v": pa.array([1.2, 2.0], pa.float64())}))

    # 1.0 -> 1.2 is within tolerance, so no cell is reported as changed
    assert a.result.summary()["changed_cells"] == 0
    a.close()


def test_audit_empty_table_start_and_no_write_errors(tmp_path):
    """Empty at start and still empty at end: nothing to audit -> clear error."""
    t = _table(tmp_path, "voidjob")

    with pytest.raises(DiffError, match="empty"):
        with audit(t, key=["id"]):
            pass
