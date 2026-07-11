"""Deployment gate — diff a pipeline's OUTPUT *before vs after* a code change,
with the INPUT held constant so the diff is attributable to the code alone.

Why this exists
---------------
A value-level diff of "yesterday's production table" vs "today's freshly built
table" is *confounded*: the difference mixes two causes —

  1. the pipeline code you changed  (what you want to see), and
  2. new/updated source rows that arrived in between  (noise).

You cannot tell them apart, so the diff is meaningless as a gate for a code change.

The fix is to remove the time variable: pin one **immutable input snapshot**, run
the OLD code and the NEW code against *that same input*, and diff the two outputs.
Because both runs read identical bytes, every difference is caused by the code
change — nothing else. (lake-sift itself is only the comparator; producing two
comparable outputs from a pinned input is what this harness adds around it.)

    pinned input  ─┬─▶  OLD sql  ─▶  output_old ─┐
    (immutable)    │                             ├─▶  lake-sift diff  ─▶  gate
                   └─▶  NEW sql  ─▶  output_new ─┘

This is the single-node, framework-free analogue of what Datafold / SQLMesh
`table_diff` do against a warehouse.

Scope
-----
The harness runs *SQL* transforms on a single DuckDB node — the same engine
lake-sift diffs with. Inputs are pinned Parquet files (immutable by nature — the
reproducible-diff sweet spot). A real deployment pins whatever its sources are
(an Iceberg snapshot id, a Delta version) and exports/reads them the same way.

Usage
-----
    # run the built-in, zero-setup demo
    python run_gate.py --demo

    # gate a real change: same pinned inputs, two model versions
    python run_gate.py \
        --input orders=./_pinned/orders.parquet \
        --old model_old.sql --new model_new.sql \
        --key order_id

Exit codes match the lake-sift CLI: 0 = identical, 1 = differences, 2 = error.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path
from typing import Mapping, Sequence

import duckdb
from rich.console import Console

from lakesift import ParquetSource, diff
from lakesift.core import DiffError
from lakesift.render.human import render_human


def materialize(sql: str, inputs: Mapping[str, str], out_path: str) -> None:
    """Run `sql` against the pinned `inputs` and write the result to `out_path`.

    `inputs` maps each table name referenced in the query to a Parquet path. A
    fresh in-memory DuckDB engine is used per call so the two runs never share
    state — the only thing they share is the (immutable) input files.
    """
    con = duckdb.connect()
    try:
        for name, path in inputs.items():
            con.read_parquet(path).create_view(name, replace=True)
        con.sql(sql).write_parquet(out_path)
    finally:
        con.close()


def run_gate(
    *,
    inputs: Mapping[str, str],
    old_sql: str,
    new_sql: str,
    key: Sequence[str],
    workdir: str | None = None,
    exclude: Sequence[str] | None = None,
    tolerance: float | None = None,
):
    """Materialize OLD and NEW outputs from the *same* pinned inputs, then diff them.

    Returns a live `lakesift.DiffResult` — use it as a context manager (or call
    ``.close()``), because it streams rows/cells over a live DuckDB connection.

    The two outputs are produced from byte-identical inputs, so the returned diff
    is attributable entirely to the difference between `old_sql` and `new_sql`.
    """
    work = Path(workdir) if workdir else Path(tempfile.mkdtemp(prefix="lake-sift-gate-"))
    work.mkdir(parents=True, exist_ok=True)
    old_out = work / "output_old.parquet"
    new_out = work / "output_new.parquet"

    materialize(old_sql, inputs, str(old_out))
    materialize(new_sql, inputs, str(new_out))

    return diff(
        ParquetSource(str(old_out)),
        ParquetSource(str(new_out)),
        key=list(key),
        exclude=list(exclude) if exclude else None,
        tolerance=tolerance,
    )


# --------------------------------------------------------------------------- CLI


def _parse_inputs(items: list[str] | None) -> dict[str, str]:
    """`--input NAME=PATH` entries -> {name: parquet-path}."""
    out: dict[str, str] = {}
    for item in items or []:
        name, sep, path = item.partition("=")
        name, path = name.strip(), path.strip()
        if not sep or not name or not path:
            raise DiffError(f"--input must be NAME=PATH: {item!r}")
        out[name] = path
    return out


def _write_demo(work: Path) -> tuple[dict[str, str], str, str, list[str]]:
    """Fabricate a pinned input + two model versions so the gate runs with zero setup.

    The pinned `orders` table is written once and read by both models, so any
    difference in the gate output comes purely from the model change:
      * NEW includes refunded orders too (was: paid only)   -> added rows
      * NEW rounds `amount` to whole units                  -> changed cells
    """
    con = duckdb.connect()
    try:
        con.sql(
            """
            SELECT * FROM (VALUES
                (1, 12.40, 'paid'),
                (2, 99.90, 'paid'),
                (3, 45.55, 'refunded'),
                (4, 10.00, 'pending'),
                (5,  8.25, 'paid')
            ) AS t(order_id, amount, status)
            """
        ).write_parquet(str(work / "orders.parquet"))
    finally:
        con.close()

    old_sql = (
        "-- revenue per PAID order (pipeline BEFORE the change)\n"
        "SELECT order_id, amount, status FROM orders WHERE status = 'paid'"
    )
    new_sql = (
        "-- pipeline AFTER the change: include refunds; round amount to whole units\n"
        "SELECT order_id, round(amount) AS amount, status\n"
        "FROM orders WHERE status IN ('paid', 'refunded')"
    )
    inputs = {"orders": str(work / "orders.parquet")}
    return inputs, old_sql, new_sql, ["order_id"]


def _read_sql(path: str) -> str:
    try:
        return Path(path).read_text(encoding="utf-8")
    except OSError as e:
        raise DiffError(f"cannot read SQL file {path!r}: {e}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Deployment gate: diff a pipeline's output before vs after a "
        "code change, with the input pinned so the diff is attributable to the code."
    )
    parser.add_argument(
        "--input", action="append", metavar="NAME=PATH",
        help="A pinned input table (repeatable): NAME is referenced in the SQL, PATH is a Parquet file.",
    )
    parser.add_argument("--old", metavar="FILE", help="SQL file for the OLD (current) pipeline.")
    parser.add_argument("--new", metavar="FILE", help="SQL file for the NEW (proposed) pipeline.")
    parser.add_argument("--key", "-k", help="Row identity key (comma-separated).")
    parser.add_argument("--exclude", "-x", help="Columns to exclude from comparison (comma-separated).")
    parser.add_argument("--tolerance", "-t", type=float, help="Numeric tolerance (equal when abs(l-r) <= tol).")
    parser.add_argument("--demo", action="store_true", help="Run the built-in, zero-setup demo.")
    args = parser.parse_args(argv)

    console = Console()
    err = Console(stderr=True)

    try:
        if args.demo:
            work = Path(tempfile.mkdtemp(prefix="lake-sift-gate-demo-"))
            inputs, old_sql, new_sql, key = _write_demo(work)
            exclude = None
            tolerance = None
            console.print(f"[dim]demo: pinned input + two model versions written to {work}[/dim]\n")
        else:
            if not (args.input and args.old and args.new and args.key):
                err.print("[red]error:[/red] --input, --old, --new and --key are required (or use --demo).")
                return 2
            inputs = _parse_inputs(args.input)
            old_sql = _read_sql(args.old)
            new_sql = _read_sql(args.new)
            key = [c.strip() for c in args.key.split(",") if c.strip()]
            exclude = [c.strip() for c in args.exclude.split(",") if c.strip()] if args.exclude else None
            tolerance = args.tolerance

        with run_gate(
            inputs=inputs, old_sql=old_sql, new_sql=new_sql,
            key=key, exclude=exclude, tolerance=tolerance,
        ) as result:
            render_human(result, console=console)
            empty = result.is_empty()
    except Exception as e:
        err.print(f"[red]error:[/red] {e}")
        return 2

    return 0 if empty else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
