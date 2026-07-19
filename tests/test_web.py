"""Web UI tests — the FastAPI dashboard over the diff library.

Skipped entirely when the optional [web] extra is not installed.
"""

from __future__ import annotations

import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("multipart")  # python-multipart, for form parsing

from fastapi.testclient import TestClient  # noqa: E402

from lakesift.web.runner import RunRequest, pair_kind, source_kind  # noqa: E402
from lakesift.web.store import RunStore  # noqa: E402


def _write(path, table):
    pq.write_table(pa.table(table), path)
    return str(path)


@pytest.fixture()
def sources(tmp_path):
    left = _write(tmp_path / "before.parquet", {"id": [1, 2, 3], "amt": [10, 20, 30]})
    right = _write(tmp_path / "after.parquet", {"id": [2, 3, 4], "amt": [20, 99, 40]})
    return left, right


@pytest.fixture()
def client(tmp_path):
    store = RunStore(tmp_path / "history.db")
    from lakesift.web.app import create_app

    c = TestClient(create_app(store))
    c.store = store  # type: ignore[attr-defined]
    return c


def _wait(store, run_id, timeout=10.0):
    """Block until the background worker finishes the run (or time out)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        run = store.get(run_id)
        if run["status"] != "running":
            return run
        time.sleep(0.02)
    raise AssertionError(f"run {run_id} did not finish in {timeout}s")


def _submit(client, **data):
    r = client.post("/runs", data=data, follow_redirects=False)
    assert r.status_code == 303, r.text
    run_id = int(r.headers["location"].rsplit("/", 1)[1])
    return _wait(client.store, run_id)


# --- source classification ---
def test_source_kind():
    assert source_kind("a.parquet") == "parquet"
    assert source_kind("iceberg:cat/ns.t") == "iceberg"
    assert source_kind("delta:/path") == "delta"
    assert source_kind("sql:q.sql") == "sql"
    assert pair_kind("a.parquet", "b.parquet") == "parquet"
    assert pair_kind("a.parquet", "delta:/p") == "mixed"


def test_run_request_roundtrip():
    req = RunRequest(mode="diff", left="a", right="b", key=["id"], tolerance=0.5)
    assert RunRequest.from_params(req.to_params()) == req


# --- routes ---
def test_index_redirects(client):
    r = client.get("/", follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == "/runs"


def test_empty_runs_list(client):
    r = client.get("/runs")
    assert r.status_code == 200 and "No runs yet" in r.text


def test_new_form(client):
    assert client.get("/runs/new").status_code == 200


def test_diff_run(client, sources):
    left, right = sources
    run = _submit(client, mode="diff", left=left, right=right, key="id")
    assert run["status"] == "differences"
    assert run["summary"] == {
        "added": 1, "removed": 1, "changed": 1, "changed_cells": 1, "schema_changes": 0
    }
    assert run["source_kind"] == "parquet"
    assert len(run["detail"]["added"]) == 1
    assert len(run["detail"]["removed"]) == 1
    # the run shows up on the list and its detail/json render
    assert f"#{run['id']}" in client.get("/runs").text
    assert client.get(f"/runs/{run['id']}").status_code == 200
    assert client.get(f"/runs/{run['id']}/json").json()["status"] == "differences"


def test_identical_diff(client, tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1, 2], "v": [1, 2]})
    b = _write(tmp_path / "b.parquet", {"id": [1, 2], "v": [1, 2]})
    run = _submit(client, mode="diff", left=a, right=b, key="id")
    assert run["status"] == "identical"


def test_diff_requires_key(client, sources):
    left, right = sources
    r = client.post("/runs", data={"mode": "diff", "left": left, "right": right, "key": ""})
    assert r.status_code == 400 and "key is required" in r.text


def test_missing_source_is_error_run(client, sources):
    left, _ = sources
    run = _submit(client, mode="diff", left=left, right="/no/such.parquet", key="id")
    assert run["status"] == "error" and run["error"]


def test_schema_run(client, tmp_path):
    a = _write(tmp_path / "a.parquet", {"id": [1], "v": [1]})
    b = _write(tmp_path / "b.parquet", {"id": [1], "w": ["x"]})
    run = _submit(client, mode="schema", left=a, right=b)
    assert run["status"] == "schema"
    kinds = {c["kind"] for c in run["detail"]["schema_changes"]}
    assert kinds == {"added", "removed"}


def test_rerun_clones_params(client, sources):
    left, right = sources
    first = _submit(client, mode="diff", left=left, right=right, key="id")
    r = client.post(f"/runs/{first['id']}/rerun", follow_redirects=False)
    assert r.status_code == 303
    new_id = int(r.headers["location"].rsplit("/", 1)[1])
    assert new_id != first["id"]
    again = _wait(client.store, new_id)
    assert again["summary"] == first["summary"]


def test_delete_run(client, sources):
    left, right = sources
    run = _submit(client, mode="diff", left=left, right=right, key="id")
    r = client.post(f"/runs/{run['id']}/delete", follow_redirects=False)
    assert r.status_code == 303 and r.headers["location"] == "/runs"
    assert client.store.get(run["id"]) is None
    assert client.get(f"/runs/{run['id']}").status_code == 404


def test_edit_and_rerun_prefills_form(client, sources):
    left, right = sources
    run = _submit(client, mode="diff", left=left, right=right, key="id", tolerance="0.5")
    r = client.get(f"/runs/new?from_={run['id']}")
    assert r.status_code == 200
    # the stored operands come back pre-filled into the form
    assert left in r.text and right in r.text and "0.5" in r.text


def test_list_has_statbar_and_favicon(client, sources):
    left, right = sources
    _submit(client, mode="diff", left=left, right=right, key="id")
    html = client.get("/runs").text
    assert "statbar" in html and 'rel="icon"' in html


def test_help_modal_on_every_page(client, sources):
    left, right = sources
    run = _submit(client, mode="diff", left=left, right=right, key="id")
    for url in ("/runs", "/runs/new", f"/runs/{run['id']}"):
        html = client.get(url).text
        assert 'id="ls-help"' in html and "lsHelp(true)" in html, url


def test_environment_page(client):
    html = client.get("/environment").text
    assert "Connections" in html and "pyiceberg.yaml" in html
    assert "Iceberg (pyiceberg)" in html  # adapter row present


def test_environment_never_leaks_secret(client, monkeypatch):
    """A read-only panel must show that creds are set, never their value."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE12345")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "s3cr3t-VALUE-should-never-render")
    monkeypatch.setenv("AWS_REGION", "ap-northeast-2")
    html = client.get("/environment").text
    assert "detected" in html and "ap-northeast-2" in html  # presence + region shown
    assert "s3cr3t-VALUE-should-never-render" not in html
    assert "AKIAEXAMPLE12345" not in html


def test_parquet_remote_detection():
    from lakesift.sources.parquet import _is_remote

    assert _is_remote("s3://bucket/data/*.parquet")
    assert _is_remote("https://host/data.parquet")
    assert not _is_remote("/local/data.parquet")
    assert not _is_remote("data/*.parquet")


def test_redact_uri_strips_credentials():
    from lakesift.web.environment import _redact_uri

    assert _redact_uri("https://user:pass@catalog.example.com/v1") == "https://catalog.example.com/v1"
    assert _redact_uri("s3://my-bucket/warehouse") == "s3://my-bucket/warehouse"


def test_run_not_found(client):
    assert client.get("/runs/9999").status_code == 404
    assert client.get("/runs/9999/json").status_code == 404
