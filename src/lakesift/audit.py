"""audit() — wrap a job and diff what it did to an Iceberg table.

A value diff of "before vs after" is the natural audit of a batch job: run the job,
then show which rows it added, removed, or changed. The boilerplate is always the same —
stamp the table's snapshot before the job, stamp it again after, diff the two. This
context manager does exactly that:

    from lakesift import audit

    with audit(table, key=["id"]) as a:
        run_my_job()          # commits one or more snapshots to `table`

    result = a.result         # DiffResult of the table before vs after the block
    if not result.is_empty():
        print(result.summary())

It is orchestrator-agnostic: the block is just Python, so it fits an Airflow
`PythonOperator`, a Dagster op, or a plain cron script equally. Unlike the `@^` operand
(which infers "before" as the current snapshot's parent), `audit()` records the real
before/after snapshot ids, so it stays exact even when the job commits *several*
snapshots (e.g. a pyiceberg `overwrite`, which emits a DELETE then an APPEND) — every
commit in between is captured.

Scope and caveats:
- The table must be an Iceberg table (a PyIceberg `Table`). Iceberg is where "before" is
  cheaply addressable: snapshots are immutable and every commit records its parent.
- Isolation is by wall-clock window, not by author. If another writer commits to the same
  table while the block runs, its rows are attributed to this audit too. For hard
  isolation, have the job write to its own branch and diff that (Write-Audit-Publish).
- If the block raises, no diff is taken and the exception propagates — a failed job has no
  meaningful "after" to audit.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from lakesift.core import DiffError, diff
from lakesift.sources.iceberg import IcebergSource

if TYPE_CHECKING:
    import duckdb
    from pyiceberg.table import Table

    from lakesift.result import DiffResult


class _EmptySource:
    """A source with a schema but no rows — the 'before' when the table starts empty.

    Lets `audit()` handle a first load (no snapshot yet at block start): every row the job
    writes then shows up as added, against the after-schema so key validation still passes.
    """

    def __init__(self, arrow_schema: Any):
        self._schema = arrow_schema

    def arrow_schema(self) -> Any:
        return self._schema

    def to_relation(
        self, con: "duckdb.DuckDBPyConnection", *, columns: Sequence[str] | None = None
    ) -> "duckdb.DuckDBPyRelation":
        schema = self._schema
        if columns is not None:
            import pyarrow as pa

            schema = pa.schema([schema.field(c) for c in columns])
        return con.from_arrow(schema.empty_table())


class Audit:
    """The live handle for an `audit(...)` block. Get one via `with audit(...) as a:`.

    After the block exits cleanly, `result` holds the before-vs-after `DiffResult`.
    That result owns a live DuckDB connection; call `close()` (or use the diff options to
    keep it small and let the process exit) when done. `before_snapshot_id` /
    `after_snapshot_id` expose the exact snapshots that were compared.
    """

    def __init__(self, table: "Table", key: Sequence[str], **diff_opts: Any):
        self.table = table
        self.key = list(key)
        self._diff_opts = diff_opts
        self.before_snapshot_id: int | None = None
        self.after_snapshot_id: int | None = None
        self.result: "DiffResult | None" = None

    def _current_snapshot_id(self) -> int | None:
        snap = self.table.current_snapshot()
        return snap.snapshot_id if snap is not None else None

    def __enter__(self) -> "Audit":
        self.before_snapshot_id = self._current_snapshot_id()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is not None:
            # The job failed — there is no meaningful "after". Let the error propagate.
            return False

        # See the job's commits, including any made by a separate process (e.g. Spark).
        # An in-process pyiceberg write already updates the object, so this is a no-op then.
        refresh = getattr(self.table, "refresh", None)
        if refresh is not None:
            refreshed = refresh()
            if refreshed is not None:
                self.table = refreshed

        self.after_snapshot_id = self._current_snapshot_id()
        if self.after_snapshot_id is None:
            raise DiffError("audit() found no snapshot after the block; the table is empty.")

        after = IcebergSource(self.table, snapshot_id=self.after_snapshot_id)
        if self.before_snapshot_id is None:
            # The table was empty when the block started: everything is an addition.
            before: Any = _EmptySource(after.arrow_schema())
        else:
            before = IcebergSource(self.table, snapshot_id=self.before_snapshot_id)

        self.result = diff(before, after, self.key, **self._diff_opts)
        return False

    def close(self) -> None:
        """Release the diff result's DuckDB connection, if a result was produced."""
        if self.result is not None:
            self.result.close()


def audit(table: "Table", key: Sequence[str], **diff_opts: Any) -> Audit:
    """Audit what a job does to an Iceberg `table`: diff it before vs after the block.

    Use as a context manager around the job; read `.result` afterwards::

        with audit(table, key=["id"]) as a:
            run_job()
        if not a.result.is_empty():
            raise RuntimeError(a.result.summary())

    `key` is the primary key used to align rows (same meaning as `diff()`). Extra keyword
    arguments are forwarded verbatim to `diff()` — `exclude`, `columns`, `allow_duplicates`,
    `tolerance`, `ignore_case`.
    """
    return Audit(table, key, **diff_opts)
