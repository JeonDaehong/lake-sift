"""Metadata-only preview. The whole module is skipped if pyiceberg isn't installed.

The bounds `preview()` reports are claims about what a real `diff()` would find, so most
tests here assert the claim *and* cross-check it against an actual diff of the same two
snapshots. A bound that doesn't hold is the one bug this feature cannot have.
"""

from __future__ import annotations

import os

import pyarrow as pa
import pytest

pytest.importorskip("pyiceberg")
from pyiceberg.catalog.sql import SqlCatalog  # noqa: E402
from pyiceberg.expressions import EqualTo  # noqa: E402
from pyiceberg.transforms import IdentityTransform  # noqa: E402

from lakesift import IcebergSource, ParquetSource, diff, preview  # noqa: E402
from lakesift.core import DiffError  # noqa: E402


def _table(tmp_path, name: str, data: pa.Table, *, partition_on: str | None = None):
    """Create one Iceberg table in a local SQL catalog, optionally partitioned."""
    wh = tmp_path / f"wh_{name}"
    os.makedirs(wh, exist_ok=True)
    cat = SqlCatalog(
        name,
        uri=f"sqlite:///{tmp_path / (name + '.db')}",
        warehouse="file://" + str(wh).replace("\\", "/"),
    )
    cat.create_namespace("ns")
    t = cat.create_table(f"ns.{name}", schema=data.schema)
    if partition_on:
        with t.update_spec() as us:
            us.add_field(partition_on, IdentityTransform(), partition_on)
    t.append(data)
    return t


def _orders(ids, dt, amounts, statuses):
    return pa.table(
        {
            "order_id": pa.array(ids, pa.int64()),
            "dt": [dt] * len(ids),
            "amount": pa.array(amounts, pa.float64()),
            "status": statuses,
        }
    )


@pytest.fixture()
def orders(tmp_path):
    """A dt-partitioned table with two snapshots' worth of history.

    Returns (table, snap_a) where snap_a holds dt=2026-07-13 and dt=2026-07-14.
    """
    t = _table(tmp_path, "orders", _orders([1, 2], "2026-07-13", [10.0, 20.0], ["paid", "paid"]),
               partition_on="dt")
    t.append(_orders([3, 4], "2026-07-14", [30.0, 40.0], ["paid", "pending"]))
    t.refresh()
    return t, t.current_snapshot().snapshot_id


def _snap(t):
    """Take a fresh snapshot id after a write."""
    t.refresh()
    return t.current_snapshot().snapshot_id


def test_identical_snapshots_are_provably_identical(orders):
    """The strong claim: same snapshot on both sides, nothing to scan, no data read."""
    t, a = orders
    p = preview(IcebergSource(t, snapshot_id=a), IcebergSource(t, snapshot_id=a), key=["order_id"])
    assert p.is_empty()
    assert p.rows_to_scan == 0 and p.bytes_to_scan == 0
    assert p.files_differing == 0
    assert p.files_shared == p.files_left == p.files_right


def test_shared_files_are_excluded_from_the_scan(orders):
    """Appending a file leaves the existing files shared, so only the new one is scanned."""
    t, a = orders
    t.append(_orders([5], "2026-07-15", [50.0], ["new"]))
    b = _snap(t)

    p = preview(IcebergSource(t, snapshot_id=a), IcebergSource(t, snapshot_id=b), key=["order_id"])
    assert not p.is_empty()
    assert p.files_shared == 2  # the two original data files, untouched
    assert p.files_differing == 1  # only the newly appended file
    assert p.rows_to_scan == 1  # ... and only its single row
    assert p.rows_total == 9  # vs 4 + 5 rows for a full diff
    assert 0 < p.scan_fraction < 0.2


def test_append_is_proven_pure_without_reading_data(orders):
    """Disjoint key ranges prove no existing row can have been modified."""
    t, a = orders
    t.append(_orders([5], "2026-07-15", [50.0], ["new"]))
    b = _snap(t)

    p = preview(IcebergSource(t, snapshot_id=a), IcebergSource(t, snapshot_id=b), key=["order_id"])
    assert p.is_pure_append()
    assert p.max_changed_rows == 0
    assert p.provably_added == 1
    assert p.provably_removed == 0

    # the proof matches what a real diff actually finds
    with diff(IcebergSource(t, snapshot_id=a), IcebergSource(t, snapshot_id=b), key=["order_id"]) as r:
        assert r.summary() == {
            "added": 1, "removed": 0, "changed": 0, "changed_cells": 0, "schema_changes": 0,
        }


def test_overwrite_bounds_hold_against_the_real_diff(orders):
    """An in-place edit overlaps on key, so it is reported as 'may have changed' — and the
    upper bound must actually bound the real diff."""
    t, a = orders
    t.overwrite(_orders([1, 2], "2026-07-13", [10.0, 999.0], ["paid", "REFUNDED"]),
                overwrite_filter=EqualTo("dt", "2026-07-13"))
    b = _snap(t)

    p = preview(IcebergSource(t, snapshot_id=a), IcebergSource(t, snapshot_id=b), key=["order_id"])
    assert not p.is_pure_append()  # keys overlap -> cannot rule out a modification
    assert p.max_changed_rows == 2

    with diff(IcebergSource(t, snapshot_id=a), IcebergSource(t, snapshot_id=b), key=["order_id"]) as r:
        s = r.summary()
    assert s["changed"] <= p.max_changed_rows  # the upper bound holds
    assert s["added"] >= p.provably_added  # the lower bounds hold
    assert s["removed"] >= p.provably_removed


def test_partitions_touched_are_listed(orders):
    t, a = orders
    t.append(_orders([5], "2026-07-15", [50.0], ["new"]))
    b = _snap(t)

    p = preview(IcebergSource(t, snapshot_id=a), IcebergSource(t, snapshot_id=b), key=["order_id"])
    assert p.partitions_touched == [{"dt": "2026-07-15"}]


def test_without_key_reports_cost_but_no_proofs(orders):
    """The file-set facts don't need a key; the key-range proofs do."""
    t, a = orders
    t.append(_orders([5], "2026-07-15", [50.0], ["new"]))
    b = _snap(t)

    p = preview(IcebergSource(t, snapshot_id=a), IcebergSource(t, snapshot_id=b))
    assert p.rows_to_scan == 1  # cost is still known
    assert not p.key_pruned
    assert not p.is_pure_append()  # no key -> nothing is proven about modifications
    assert p.max_changed_rows == 0


def test_branch_preview_for_wap(orders):
    """Write-Audit-Publish: preview the staging branch against main before merging."""
    t, a = orders
    t.manage_snapshots().create_branch(a, "staging").commit()
    t.refresh()
    t.append(_orders([9], "2026-07-16", [90.0], ["new"]), branch="staging")
    t.refresh()

    p = preview(IcebergSource(t, ref="main"), IcebergSource(t, ref="staging"), key=["order_id"])
    assert p.is_pure_append()
    assert p.provably_added == 1


def test_schema_change_between_tables_is_reported(tmp_path):
    """A dropped column shows up in the preview, and keeps it from claiming 'identical'."""
    data = _orders([1], "2026-07-13", [10.0], ["paid"])
    left = _table(tmp_path, "sl", data)
    right = _table(tmp_path, "sr", data.drop_columns(["status"]))

    p = preview(IcebergSource(left), IcebergSource(right), key=["order_id"])
    assert [(c.column, c.kind) for c in p.schema_changes] == [("status", "removed")]
    assert not p.is_empty()


def test_unrelated_tables_share_nothing(tmp_path):
    """Two different tables share no files, so the preview says a full scan is needed."""
    left = _table(tmp_path, "l", _orders([1, 2], "2026-07-13", [10.0, 20.0], ["paid", "paid"]))
    right = _table(tmp_path, "r", _orders([1, 2], "2026-07-13", [10.0, 20.0], ["paid", "paid"]))

    p = preview(IcebergSource(left), IcebergSource(right), key=["order_id"])
    assert p.files_shared == 0
    assert p.rows_to_scan == p.rows_total  # nothing can be pruned
    assert not p.is_empty()  # identical *values*, but that is not provable from metadata


def test_non_iceberg_source_is_rejected(tmp_path):
    import pyarrow.parquet as pq

    t = _table(tmp_path, "ice", _orders([1], "2026-07-13", [10.0], ["paid"]))
    ppath = tmp_path / "x.parquet"
    pq.write_table(_orders([1], "2026-07-13", [10.0], ["paid"]), ppath)

    with pytest.raises(DiffError, match="data files"):
        preview(ParquetSource(str(ppath)), IcebergSource(t), key=["order_id"])


def test_data_files_reports_decoded_bounds_and_partition(orders):
    """The adapter decodes manifest bounds into real values keyed by column name."""
    t, _ = orders
    files = IcebergSource(t).data_files()
    assert len(files) == 2
    f = next(f for f in files if f.partition == {"dt": "2026-07-13"})
    assert f.record_count == 2
    assert f.size_bytes > 0
    assert f.lower_bounds["order_id"] == 1 and f.upper_bounds["order_id"] == 2
    assert f.lower_bounds["amount"] == 10.0 and f.upper_bounds["amount"] == 20.0
    assert f.delete_files == frozenset()


def test_to_dict_shape(orders):
    t, a = orders
    t.append(_orders([5], "2026-07-15", [50.0], ["new"]))
    b = _snap(t)

    d = preview(IcebergSource(t, snapshot_id=a), IcebergSource(t, snapshot_id=b), key=["order_id"]).to_dict()
    assert d["identical"] is False
    assert d["bounds"]["pure_append"] is True
    assert d["bounds"]["provably_added"] == 1
    assert d["files"]["shared"] == 2
    assert d["scan"]["rows_to_scan"] == 1
    assert d["partitions_touched"] == [{"dt": "2026-07-15"}]


# --- renderers -------------------------------------------------------------


def _rendered(p) -> str:
    from io import StringIO

    from rich.console import Console

    from lakesift.render.human import render_preview_human

    buf = StringIO()
    render_preview_human(p, console=Console(file=buf, width=120, no_color=True))
    return buf.getvalue()


def test_human_render_states_the_proof(orders):
    t, a = orders
    t.append(_orders([5], "2026-07-15", [50.0], ["new"]))
    b = _snap(t)
    out = _rendered(preview(IcebergSource(t, snapshot_id=a), IcebergSource(t, snapshot_id=b), key=["order_id"]))

    assert "blast radius" in out
    assert "2 shared" in out  # the files that never get read
    assert "1 of 9" in out  # rows to scan vs a full diff
    assert "pure append" in out
    assert "1 row" in out and "1 rows" not in out  # singular


def test_human_render_identical(orders):
    t, a = orders
    out = _rendered(preview(IcebergSource(t, snapshot_id=a), IcebergSource(t, snapshot_id=a), key=["order_id"]))
    assert "provably identical" in out


def test_human_render_without_key_points_at_it(orders):
    t, a = orders
    t.append(_orders([5], "2026-07-15", [50.0], ["new"]))
    b = _snap(t)
    out = _rendered(preview(IcebergSource(t, snapshot_id=a), IcebergSource(t, snapshot_id=b)))
    assert "pass --key" in out
    assert "provably added" not in out  # the proof section is absent without a key


def test_human_render_is_console_encodable(orders):
    """Human output must survive a Windows cp949 console (no em dash, no emoji)."""
    t, a = orders
    t.append(_orders([5], "2026-07-15", [50.0], ["new"]))
    b = _snap(t)
    out = _rendered(preview(IcebergSource(t, snapshot_id=a), IcebergSource(t, snapshot_id=b), key=["order_id"]))
    out.encode("cp949")  # raises UnicodeEncodeError if a renderer regresses


def test_markdown_render(orders):
    from lakesift.render.markdown import render_preview_markdown

    t, a = orders
    t.append(_orders([5], "2026-07-15", [50.0], ["new"]))
    b = _snap(t)
    md = render_preview_markdown(
        preview(IcebergSource(t, snapshot_id=a), IcebergSource(t, snapshot_id=b), key=["order_id"])
    )
    assert md.startswith("### lake-sift preview")
    assert "Blast radius" in md and "Proof" in md
    assert "pure append" in md


def test_markdown_render_identical(orders):
    from lakesift.render.markdown import render_preview_markdown

    t, a = orders
    md = render_preview_markdown(
        preview(IcebergSource(t, snapshot_id=a), IcebergSource(t, snapshot_id=a), key=["order_id"])
    )
    assert "Provably identical" in md


# --- soundness regressions -------------------------------------------------
# Each of these once made preview() claim more than it could prove.


def test_differing_projections_are_not_called_identical(orders):
    """Same files, different columns read -> the rows are NOT identical.

    Regression: file identity ignored the projection, so preview called these
    'provably identical' (exit 0) while a real diff reports schema changes.
    """
    t, a = orders
    left = IcebergSource(t, snapshot_id=a, selected_fields=["order_id", "amount"])
    right = IcebergSource(t, snapshot_id=a, selected_fields=["order_id", "status"])

    p = preview(left, right, key=["order_id"])
    assert not p.is_empty()
    assert p.files_shared == 0  # same paths, but not the same read

    with diff(left, right, key=["order_id"]) as r:
        assert not r.is_empty()  # the real diff agrees the sides differ


def test_identical_projections_still_share_files(orders):
    """The projection discriminates identity, but only when it actually differs."""
    t, a = orders
    left = IcebergSource(t, snapshot_id=a, selected_fields=["order_id", "amount"])
    right = IcebergSource(t, snapshot_id=a, selected_fields=["order_id", "amount"])
    assert preview(left, right, key=["order_id"]).is_empty()


def test_row_filter_is_refused_rather_than_miscounted(orders):
    """Manifests count whole files, so a filtered scan cannot be bounded.

    Regression: preview reported `provably_added` counting every row of a file, even
    when the filter would have excluded most of them — breaking the lower-bound
    guarantee. Refusing beats quietly reporting a number that isn't true.
    """
    t, a = orders
    filtered = IcebergSource(t, snapshot_id=a, row_filter=EqualTo("status", "paid"))
    with pytest.raises(DiffError, match="filtered scan"):
        preview(filtered, IcebergSource(t, snapshot_id=a), key=["order_id"])
    # ... and the filter is refused on either side
    with pytest.raises(DiffError, match="filtered scan"):
        preview(IcebergSource(t, snapshot_id=a), filtered, key=["order_id"])


def test_row_filter_still_works_for_a_real_diff(orders):
    """Refusing to *preview* a filtered source must not break diffing one."""
    t, a = orders
    left = IcebergSource(t, snapshot_id=a, row_filter=EqualTo("status", "paid"))
    right = IcebergSource(t, snapshot_id=a, row_filter=EqualTo("status", "paid"))
    with diff(left, right, key=["order_id"]) as r:
        assert r.is_empty()
