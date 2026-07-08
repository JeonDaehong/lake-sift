"""Schema-only diff (`schema_diff` / CLI `--schema-only`) — compares schemas, reads no data."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
from typer.testing import CliRunner

from lakesift import ParquetSource, schema_diff
from lakesift.cli import app

runner = CliRunner()


def _write(path, rows: dict):
    pq.write_table(pa.table(rows), path)
    return str(path)


def test_identical_schema_is_empty(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1], "v": ["a"]})
    b = _write(tmp_path / "b.parquet", {"id": [2], "v": ["b"]})  # different data, same schema
    result = schema_diff(ParquetSource(a), ParquetSource(b))
    assert result.is_empty()
    assert result.summary()["schema_changes"] == 0


def test_added_and_removed_columns(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1], "gone": ["x"]})
    b = _write(tmp_path / "b.parquet", {"id": [1], "fresh": ["y"]})
    result = schema_diff(ParquetSource(a), ParquetSource(b))
    kinds = {(c.column, c.kind) for c in result.schema_changes}
    assert ("gone", "removed") in kinds
    assert ("fresh", "added") in kinds
    assert not result.is_empty()


def test_type_change(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1], "amount": pa.array([1], pa.int32())})
    b = _write(tmp_path / "b.parquet", {"id": [1], "amount": pa.array([1.0], pa.float64())})
    result = schema_diff(ParquetSource(a), ParquetSource(b))
    changed = [c for c in result.schema_changes if c.column == "amount"]
    assert len(changed) == 1 and changed[0].kind == "type_changed"


def test_cli_schema_only_ignores_data_differences(tmp_path):
    # Rows differ, but the schema is identical -> schema-only reports no difference.
    a = _write(tmp_path / "a.parquet", {"id": [1, 2], "v": ["a", "b"]})
    b = _write(tmp_path / "b.parquet", {"id": [9, 8], "v": ["z", "y"]})
    r = runner.invoke(app, [a, b, "--schema-only"])  # no --key needed
    assert r.exit_code == 0


def test_cli_schema_only_flags_schema_change(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1], "v": ["a"]})
    b = _write(tmp_path / "b.parquet", {"id": [1], "v": ["a"], "extra": ["x"]})
    r = runner.invoke(app, [a, b, "--schema-only"])
    assert r.exit_code == 1
    assert "extra" in r.stdout


def test_cli_schema_only_needs_no_key(tmp_path):
    # Without --schema-only and without --key this is exit 2; --schema-only lifts that.
    a = _write(tmp_path / "a.parquet", {"id": [1], "v": ["a"]})
    b = _write(tmp_path / "b.parquet", {"id": [1], "v": ["a"]})
    assert runner.invoke(app, [a, b]).exit_code == 2
    assert runner.invoke(app, [a, b, "--schema-only"]).exit_code == 0
