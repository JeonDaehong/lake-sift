"""CLI behavior — the exit-code convention (0/1/2) and flags."""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from lakesift import __version__
from lakesift.cli import _source, app
from lakesift.core import DiffError
from lakesift.sources.delta import DeltaSource
from lakesift.sources.parquet import ParquetSource

runner = CliRunner()


def _write(path, rows: dict):
    pq.write_table(pa.table(rows), path)
    return str(path)


def test_exit_0_when_identical(tmp_path):
    data = {"id": [1, 2], "v": ["a", "b"]}
    a = _write(tmp_path / "a.parquet", data)
    b = _write(tmp_path / "b.parquet", data)
    r = runner.invoke(app, [a, b, "-k", "id"])
    assert r.exit_code == 0


def test_exit_1_when_diff(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1], "v": ["a"]})
    b = _write(tmp_path / "b.parquet", {"id": [1], "v": ["b"]})
    r = runner.invoke(app, [a, b, "-k", "id"])
    assert r.exit_code == 1


def test_exit_2_when_missing_key_option(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1], "v": ["a"]})
    b = _write(tmp_path / "b.parquet", {"id": [1], "v": ["a"]})
    r = runner.invoke(app, [a, b])  # no --key
    assert r.exit_code == 2


def test_exit_2_when_bad_key(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1], "v": ["a"]})
    b = _write(tmp_path / "b.parquet", {"id": [1], "v": ["a"]})
    r = runner.invoke(app, [a, b, "-k", "nope"])
    assert r.exit_code == 2


def test_exit_2_when_file_missing(tmp_path):
    b = _write(tmp_path / "b.parquet", {"id": [1], "v": ["a"]})
    r = runner.invoke(app, [str(tmp_path / "nope.parquet"), b, "-k", "id"])
    assert r.exit_code == 2


def test_version_flag(tmp_path):
    r = runner.invoke(app, ["--version"])
    assert r.exit_code == 0
    assert __version__ in r.stdout


def test_json_output_is_valid(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1, 2], "v": ["a", "b"]})
    b = _write(tmp_path / "b.parquet", {"id": [2, 3], "v": ["b", "c"]})
    r = runner.invoke(app, [a, b, "-k", "id", "--json"])
    assert r.exit_code == 1
    payload = json.loads(r.stdout)
    assert payload["summary"]["added"] == 1
    assert payload["summary"]["removed"] == 1


def test_tolerance_flag(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1], "v": [1.00]})
    b = _write(tmp_path / "b.parquet", {"id": [1], "v": [1.02]})
    # within tol 0.1 -> identical (exit 0)
    assert runner.invoke(app, [a, b, "-k", "id", "-t", "0.1"]).exit_code == 0
    # without tol -> changed (exit 1)
    assert runner.invoke(app, [a, b, "-k", "id"]).exit_code == 1


def test_ignore_case_flag(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1], "v": ["A"]})
    b = _write(tmp_path / "b.parquet", {"id": [1], "v": ["a"]})
    assert runner.invoke(app, [a, b, "-k", "id", "-i"]).exit_code == 0
    assert runner.invoke(app, [a, b, "-k", "id"]).exit_code == 1


def test_sample_limits_human_output(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1, 2, 3], "v": ["a", "b", "c"]})
    b = _write(tmp_path / "b.parquet", {"id": [1, 2, 3], "v": ["A", "B", "C"]})
    r = runner.invoke(app, [a, b, "-k", "id", "-n", "1"])
    assert r.exit_code == 1
    assert "+2 more" in r.stdout  # only 1 of 3 shown, the rest truncated


def test_negative_sample_and_top_rejected(tmp_path):
    # a negative --sample/--top is a usage error (a negative limit produced a bogus
    # "+N more" count); reject it up front with exit code 2.
    a = _write(tmp_path / "a.parquet", {"id": [1], "v": ["a"]})
    b = _write(tmp_path / "b.parquet", {"id": [1], "v": ["b"]})
    assert runner.invoke(app, [a, b, "-k", "id", "-n", "-3"]).exit_code == 2
    assert runner.invoke(app, [a, b, "-k", "id", "--top", "-1"]).exit_code == 2


def test_top_columns_shown_and_toggle(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1, 2], "a": ["x", "x"], "b": ["p", "q"]})
    b = _write(tmp_path / "b.parquet", {"id": [1, 2], "a": ["X", "Y"], "b": ["p", "q"]})
    r = runner.invoke(app, [a, b, "-k", "id"])
    assert "top changed columns" in r.stdout and "a (2)" in r.stdout
    # turning it off with --top 0 hides it
    r0 = runner.invoke(app, [a, b, "-k", "id", "--top", "0"])
    assert "top changed columns" not in r0.stdout


def test_unknown_columns_exit_2(tmp_path):
    # a --columns typo must error (exit 2), not silently report "identical" (exit 0)
    a = _write(tmp_path / "a.parquet", {"id": [1], "v": ["a"]})
    b = _write(tmp_path / "b.parquet", {"id": [1], "v": ["a"]})
    r = runner.invoke(app, [a, b, "-k", "id", "-c", "nope"])
    assert r.exit_code == 2


def test_columns_filter(tmp_path):
    # only b changes, but --columns a compares only a -> treated as identical (exit 0)
    a = _write(tmp_path / "a.parquet", {"id": [1], "a": ["x"], "b": ["p"]})
    b = _write(tmp_path / "b.parquet", {"id": [1], "a": ["x"], "b": ["q"]})
    assert runner.invoke(app, [a, b, "-k", "id", "-c", "a"]).exit_code == 0
    assert runner.invoke(app, [a, b, "-k", "id"]).exit_code == 1


# --- source spec parsing (_source) -------------------------------------------


def test_source_defaults_to_parquet():
    src = _source("data/a.parquet")
    assert isinstance(src, ParquetSource) and src.path == "data/a.parquet"


def _patch_from_catalog(monkeypatch, captured):
    def fake_from_catalog(catalog, identifier, *, snapshot_id=None, ref=None):
        captured.update(catalog=catalog, identifier=identifier, snapshot_id=snapshot_id, ref=ref)
        return object()  # validate the args without an actual catalog connection

    monkeypatch.setattr(
        "lakesift.cli.IcebergSource.from_catalog", staticmethod(fake_from_catalog)
    )


def test_source_iceberg_parses_catalog_identifier_and_snapshot(monkeypatch):
    captured = {}
    _patch_from_catalog(monkeypatch, captured)

    _source("iceberg:prod/sales.orders@123")
    assert captured == {"catalog": "prod", "identifier": "sales.orders", "snapshot_id": 123, "ref": None}

    _source("iceberg:prod/sales.orders")  # snapshot/ref omitted -> both None
    assert captured["snapshot_id"] is None and captured["ref"] is None


def test_source_iceberg_parses_branch_or_tag_ref(monkeypatch):
    # a non-integer after '@' is a branch/tag name (Write-Audit-Publish workflow)
    captured = {}
    _patch_from_catalog(monkeypatch, captured)

    _source("iceberg:prod/sales.orders@staging")
    assert captured["ref"] == "staging" and captured["snapshot_id"] is None


def test_source_iceberg_bad_format_raises():
    with pytest.raises(DiffError):
        _source("iceberg:no-slash")  # no catalog/identifier separator


def test_source_iceberg_empty_ref_raises():
    with pytest.raises(DiffError):
        _source("iceberg:prod/sales.orders@")


def test_source_delta_parses_path_and_version():
    src = _source("delta:/data/my_table@5")
    assert isinstance(src, DeltaSource)
    assert src.table == "/data/my_table" and src.version == 5

    src2 = _source("delta:s3://bucket/t")  # version omitted -> None, URI preserved
    assert isinstance(src2, DeltaSource)
    assert src2.table == "s3://bucket/t" and src2.version is None


def test_source_delta_non_integer_version_raises():
    with pytest.raises(DiffError):
        _source("delta:/data/my_table@latest")


def test_source_delta_empty_path_raises():
    with pytest.raises(DiffError):
        _source("delta:@5")


def test_source_delta_empty_version_raises():
    # a trailing '@' with no version is a usage error, not int('') blowing up
    with pytest.raises(DiffError, match="after '@' is empty"):
        _source("delta:/data/my_table@")


# --- sql: schema-prediction source (--upstream) ------------------------------


def test_parse_upstreams_and_bad_forms():
    from lakesift.cli import _parse_upstreams

    assert _parse_upstreams(["orders=o.parquet", "c = iceberg:p/n.t"]) == {
        "orders": "o.parquet",
        "c": "iceberg:p/n.t",
    }
    assert _parse_upstreams(None) == {}
    for bad in ["noequals", "=nope", "name="]:
        with pytest.raises(DiffError, match="NAME=SOURCE"):
            _parse_upstreams([bad])


def test_sql_source_needs_upstream_and_path():
    with pytest.raises(DiffError, match="upstream"):
        _source("sql:model.sql")  # no upstreams supplied
    with pytest.raises(DiffError, match="sql:"):
        _source("sql:", upstreams={"orders": "o.parquet"})  # empty path


def test_sql_source_missing_file_raises(tmp_path):
    with pytest.raises(DiffError, match="cannot read"):
        _source(f"sql:{tmp_path / 'nope.sql'}", upstreams={"orders": "o.parquet"})


def test_cli_sql_schema_prediction_end_to_end(tmp_path):
    # the model drops `discount` -> current vs predicted schema differs (exit 1)
    orders = _write(tmp_path / "orders.parquet", {"id": [1], "amount": [1], "discount": [0.1]})
    model = tmp_path / "model.sql"
    model.write_text("SELECT id, amount FROM orders", encoding="utf-8")
    r = runner.invoke(app, [orders, f"sql:{model}", "--schema-only", "-u", f"orders={orders}"])
    assert r.exit_code == 1
    assert "discount" in r.stdout


def test_cli_structural_only_ignores_type_change(tmp_path):
    orders = _write(tmp_path / "orders.parquet", {"id": [1], "amount": pa.array([1], pa.int32())})
    model = tmp_path / "m.sql"
    model.write_text("SELECT id, CAST(amount AS DOUBLE) AS amount FROM orders", encoding="utf-8")
    base = [orders, f"sql:{model}", "--schema-only", "-u", f"orders={orders}"]
    assert runner.invoke(app, base).exit_code == 1  # type change detected
    # structurally the columns are identical -> --structural-only clears it (exit 0)
    assert runner.invoke(app, base + ["--structural-only"]).exit_code == 0
