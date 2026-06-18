"""CLI 동작 검증 — exit code 규약(0/1/2)과 플래그."""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
from typer.testing import CliRunner

from lakesift.cli import app

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
    r = runner.invoke(app, [a, b])  # --key 없음
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


def test_json_output_is_valid(tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1, 2], "v": ["a", "b"]})
    b = _write(tmp_path / "b.parquet", {"id": [2, 3], "v": ["b", "c"]})
    r = runner.invoke(app, [a, b, "-k", "id", "--json"])
    assert r.exit_code == 1
    payload = json.loads(r.stdout)
    assert payload["summary"]["added"] == 1
    assert payload["summary"]["removed"] == 1


def test_columns_filter(tmp_path):
    # b 만 바뀌는데 --columns a 로 a 만 비교 → 동일 취급(exit 0)
    a = _write(tmp_path / "a.parquet", {"id": [1], "a": ["x"], "b": ["p"]})
    b = _write(tmp_path / "b.parquet", {"id": [1], "a": ["x"], "b": ["q"]})
    assert runner.invoke(app, [a, b, "-k", "id", "-c", "a"]).exit_code == 0
    assert runner.invoke(app, [a, b, "-k", "id"]).exit_code == 1
