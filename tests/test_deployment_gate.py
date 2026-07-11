"""End-to-end test for the deployment-gate example harness.

The harness (``examples/deployment-gate/run_gate.py``) runs an OLD and a NEW SQL
model against the *same pinned input*, so any diff is attributable to the model
change alone. These tests pin one Parquet input and assert exactly that property:
identical models -> empty diff (no time-drift false positives); a changed model
-> a diff that matches the code change and nothing else.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

_HARNESS = (
    Path(__file__).resolve().parents[1] / "examples" / "deployment-gate" / "run_gate.py"
)


def _load_harness():
    spec = importlib.util.spec_from_file_location("deployment_gate_harness", _HARNESS)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


run_gate = _load_harness().run_gate
main = _load_harness().main


def _pin_orders(tmp_path) -> dict[str, str]:
    """Pin one immutable input both model versions will read."""
    path = tmp_path / "orders.parquet"
    pq.write_table(
        pa.table(
            {
                "order_id": [1, 2, 3, 4, 5],
                "amount": [12.0, 100.0, 45.0, 10.0, 8.0],
                "status": ["paid", "paid", "refunded", "pending", "paid"],
            }
        ),
        path,
    )
    return {"orders": str(path)}


_PAID = "SELECT order_id, amount, status FROM orders WHERE status = 'paid'"


def test_identical_models_yield_empty_diff(tmp_path):
    # Same SQL on both sides against the same pinned input: no difference at all.
    # This is the guarantee the naive "prod vs freshly built" check cannot make.
    inputs = _pin_orders(tmp_path)
    with run_gate(
        inputs=inputs, old_sql=_PAID, new_sql=_PAID, key=["order_id"],
        workdir=str(tmp_path / "work"),
    ) as result:
        assert result.is_empty()


def test_code_change_is_fully_attributable(tmp_path):
    # NEW model: include refunds (adds a row) and add a constant column (schema add).
    # The pinned input is identical for both runs, so the diff below is caused ONLY
    # by the difference between the two queries.
    inputs = _pin_orders(tmp_path)
    new_sql = (
        "SELECT order_id, amount, status, true AS is_settled "
        "FROM orders WHERE status IN ('paid', 'refunded')"
    )
    with run_gate(
        inputs=inputs, old_sql=_PAID, new_sql=new_sql, key=["order_id"],
        workdir=str(tmp_path / "work"),
    ) as result:
        summary = result.summary()
        # exactly the refunded order (id=3) appears; nothing spuriously removed.
        assert [r["order_id"] for r in result.added] == [3]
        assert summary["removed"] == 0
        # the added `is_settled` column is reported as a schema change.
        added_cols = {c.column for c in result.schema_changes if c.kind == "added"}
        assert "is_settled" in added_cols
        assert not result.is_empty()


def test_value_change_shows_changed_cells(tmp_path):
    # NEW model scales amount: pure value change on the shared keys, no row churn.
    inputs = _pin_orders(tmp_path)
    new_sql = "SELECT order_id, amount * 2 AS amount, status FROM orders WHERE status = 'paid'"
    with run_gate(
        inputs=inputs, old_sql=_PAID, new_sql=new_sql, key=["order_id"],
        workdir=str(tmp_path / "work"),
    ) as result:
        summary = result.summary()
        assert summary["added"] == 0 and summary["removed"] == 0
        changed = {c.key["order_id"]: (c.old, c.new) for c in result.changed_cells}
        # paid orders are 1, 2, 5 -> each amount doubled.
        assert changed == {1: (12.0, 24.0), 2: (100.0, 200.0), 5: (8.0, 16.0)}


def test_main_demo_returns_diff_exit_code():
    # The built-in --demo path runs end to end and reports differences (exit 1).
    assert main(["--demo"]) == 1


def test_main_missing_args_is_usage_error():
    assert main([]) == 2
