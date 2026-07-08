"""SqlSchemaSource — predict a query's output schema statically (SQLGlot), then diff it.

The predicted schema is compared against the *current* (live) schema with schema_diff, so
a pipeline change can be gated before it runs. Orientation matches the rest of the tool:
left = current/before, right = predicted/after.
"""

from __future__ import annotations

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lakesift import ParquetSource, SqlSchemaSource, schema_diff
from lakesift.core import DiffError


def _write(path, rows: dict):
    pq.write_table(pa.table(rows), path)
    return str(path)


def test_predicts_dropped_column(tmp_path):
    orders = _write(tmp_path / "orders.parquet", {"id": [1], "amount": [1], "discount": [0.1]})
    pred = SqlSchemaSource("SELECT id, amount FROM orders", {"orders": ParquetSource(orders)})
    res = schema_diff(ParquetSource(orders), pred)  # current vs predicted-new
    kinds = {(c.column, c.kind) for c in res.schema_changes}
    assert ("discount", "removed") in kinds
    assert not res.is_empty()


def test_select_star_is_identical(tmp_path):
    orders = _write(tmp_path / "orders.parquet", {"id": [1], "amount": [1], "s": ["p"]})
    pred = SqlSchemaSource("SELECT * FROM orders", {"orders": ParquetSource(orders)})
    assert schema_diff(ParquetSource(orders), pred).is_empty()


def test_predicts_added_column(tmp_path):
    orders = _write(tmp_path / "orders.parquet", {"id": [1], "amount": [1]})
    pred = SqlSchemaSource(
        "SELECT id, amount, amount AS amount_copy FROM orders", {"orders": ParquetSource(orders)}
    )
    res = schema_diff(ParquetSource(orders), pred)
    assert ("amount_copy", "added") in {(c.column, c.kind) for c in res.schema_changes}


def test_type_change_and_structural_only(tmp_path):
    orders = _write(tmp_path / "orders.parquet", {"id": [1], "amount": pa.array([1], pa.int32())})
    pred = SqlSchemaSource(
        "SELECT id, CAST(amount AS DOUBLE) AS amount FROM orders",
        {"orders": ParquetSource(orders)},
    )
    res = schema_diff(ParquetSource(orders), pred)
    changed = [c for c in res.schema_changes if c.column == "amount"]
    assert len(changed) == 1 and changed[0].kind == "type_changed"
    # Structurally the two schemas are identical (same columns), so compare_types=False clears it.
    assert schema_diff(ParquetSource(orders), pred, compare_types=False).is_empty()


def test_upstream_given_as_pyarrow_schema():
    sch = pa.schema([("id", pa.int64()), ("amount", pa.int64())])
    pred = SqlSchemaSource("SELECT id FROM orders", {"orders": sch})
    assert pred.arrow_schema().names == ["id"]


def test_unknown_type_is_null_typed(tmp_path):
    orders = _write(tmp_path / "orders.parquet", {"id": [1], "amount": [1]})
    pred = SqlSchemaSource(
        "SELECT id, some_unknown_udf(amount) AS u FROM orders", {"orders": ParquetSource(orders)}
    )
    out = pred.arrow_schema()
    assert "u" in out.names
    assert out.field("u").type == pa.null()  # unresolved -> present but null-typed


def test_to_relation_rejects_materialization():
    pred = SqlSchemaSource("SELECT 1 AS x", {})
    with pytest.raises(DiffError):
        pred.to_relation(duckdb.connect())
