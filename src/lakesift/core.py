"""diff engine — takes key/options and compares via DuckDB SQL.

Python only generates/orchestrates SQL; the heavy comparison is delegated entirely to
DuckDB. NULL-equality comparison (treating `NULL == NULL` as equal) is handled by SQL's
`IS [NOT] DISTINCT FROM`. Floats use exact-match comparison in v0 — numeric tolerance is
in v0.2.

Memory: row/cell deltas are not materialized into Python lists. Counts are computed
first with aggregate queries, and the actual rows/cells are streamed in batches through a
DuckDB cursor when `DiffResult` accesses them. Because of this the result owns a live
connection, so using it as a context manager is the safe choice.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterator, Sequence

import duckdb

from lakesift.result import CellChange, DiffResult, SchemaChange

if TYPE_CHECKING:
    from lakesift.sources.base import Source

# Number of rows pulled from a cursor at once. Too small = round-trip cost, too large = memory.
_BATCH = 2048


class DiffError(Exception):
    """An error that prevents comparison (mapped to exit code 2 in the CLI)."""


def _q(name: str) -> str:
    """Safely quote an identifier."""
    return '"' + name.replace('"', '""') + '"'


def _schema_of(rel: "duckdb.DuckDBPyRelation") -> dict[str, str]:
    """Relation column name -> type string (preserving input order)."""
    return {name: str(t) for name, t in zip(rel.columns, rel.types)}


def _probe_schema(con, source: "Source") -> dict[str, str]:
    """Get the source's column name -> type without reading data (when possible).

    If the source provides `arrow_schema()`, derive DuckDB types from an empty Arrow
    table (Iceberg/Delta: metadata only, no full scan). Otherwise read from the
    `to_relation` relation (Parquet is lazy, so only the footer is read — cheap). Either
    way the type strings are unified to DuckDB's representation, so left/right comparison
    stays consistent.
    """
    arrow_schema = getattr(source, "arrow_schema", None)
    if arrow_schema is not None:
        return _schema_of(con.from_arrow(arrow_schema().empty_table()))
    return _schema_of(source.to_relation(con))


def _has_duplicate_keys(con, view: str, key: Sequence[str]) -> bool:
    cols = ", ".join(_q(k) for k in key)
    sql = f"SELECT 1 FROM {view} GROUP BY {cols} HAVING count(*) > 1 LIMIT 1"
    return con.execute(sql).fetchone() is not None


# Classify DuckDB type strings (ignoring case/parameters).
_NUMERIC_HINTS = ("INT", "DECIMAL", "NUMERIC", "DOUBLE", "FLOAT", "REAL", "HUGEINT")
_TEXT_HINTS = ("VARCHAR", "CHAR", "TEXT", "STRING")


def _is_numeric(t: str) -> bool:
    u = t.upper()
    return any(h in u for h in _NUMERIC_HINTS)


def _is_text(t: str) -> bool:
    u = t.upper()
    return any(h in u for h in _TEXT_HINTS)


def _diff_pred(col: str, ltype: str, *, tolerance: float | None, ignore_case: bool) -> str:
    """SQL boolean expression that is TRUE when a cell is considered 'different'.

    The default is `IS DISTINCT FROM` (NULL==NULL equal). Options:
    - tolerance: numeric columns are equal when `abs(l-r) <= tol`.
    - ignore_case: text columns are compared case-insensitively.
    Only one applies, based on the column type (left side).
    """
    lc, rc = f"l.{_q(col)}", f"r.{_q(col)}"
    if tolerance is not None and _is_numeric(ltype):
        tol = repr(float(tolerance))  # a float, so inlining is injection-safe
        # different = NOT (both NULL, or both non-NULL and within tol)
        return (
            f"NOT ( ({lc} IS NULL AND {rc} IS NULL) OR "
            f"({lc} IS NOT NULL AND {rc} IS NOT NULL AND abs({lc} - {rc}) <= {tol}) )"
        )
    if ignore_case and _is_text(ltype):
        return f"lower({lc}) IS DISTINCT FROM lower({rc})"
    return f"{lc} IS DISTINCT FROM {rc}"


def _stream_dicts(con, sql: str):
    """Generator factory yielding each result row as a dict (fresh cursor per access)."""

    def factory() -> Iterator[dict[str, Any]]:
        cur = con.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        while True:
            rows = cur.fetchmany(_BATCH)
            if not rows:
                break
            for row in rows:
                yield dict(zip(cols, row))

    return factory


def _stream_cells(con, key: list[str], compare_cols: list[str], key_join: str, preds: dict[str, str]):
    """Generator factory streaming changed cells, one cursor per column in turn.

    Each column may have different old/new types; merging into one query would coerce
    them to a common type and distort values. So we stream per-column queries in sequence
    to preserve the original types.
    """
    key_sel = ", ".join(f"l.{_q(k)} AS {_q(k)}" for k in key)

    def factory() -> Iterator[CellChange]:
        for c in compare_cols:
            qc = _q(c)
            sql = (
                f"SELECT {key_sel}, l.{qc} AS old_val, r.{qc} AS new_val "
                f"FROM l JOIN r ON {key_join} "
                f"WHERE {preds[c]}"
            )
            cur = con.cursor()
            cur.execute(sql)
            while True:
                rows = cur.fetchmany(_BATCH)
                if not rows:
                    break
                for row in rows:
                    keyvals = {k: row[i] for i, k in enumerate(key)}
                    yield CellChange(key=keyvals, column=c, old=row[-2], new=row[-1])

    return factory


def diff(
    left: "Source",
    right: "Source",
    key: Sequence[str],
    *,
    exclude: Sequence[str] | None = None,
    columns: Sequence[str] | None = None,
    allow_duplicates: bool = False,
    tolerance: float | None = None,
    ignore_case: bool = False,
) -> DiffResult:
    """Compare `left` and `right` cell by cell, keyed on `key`.

    The returned `DiffResult` owns a live DuckDB connection. Since it streams rows/cells
    to the end, use it as `with diff(...) as result:` or call `result.close()` to close
    the connection.

    Raises:
        DiffError: comparison is impossible (missing/absent key, duplicate keys with
            allow_duplicates=False, etc.).
    """
    key = list(key)
    if not key:
        # v0: no set-diff fallback. Stop with an explicit error.
        raise DiffError("a key is required (set-diff fallback is not supported yet).")

    exclude_set = set(exclude or [])
    columns_filter = set(columns) if columns else None
    # Projection is only enabled when the user narrows the comparison (--columns/--exclude).
    # When enabled, only key + compared columns are pushed down to the scan, so unused
    # columns are never read. Side effect: added/removed rows then show only those columns
    # (schema-change detection is still based on the full schema).
    projection_active = columns_filter is not None or bool(exclude_set)

    con = duckdb.connect()  # in-memory; on success DiffResult takes ownership.
    try:
        if projection_active:
            # Get the schema cheaply before materializing data, to decide which columns to read.
            lschema = _probe_schema(con, left)
            rschema = _probe_schema(con, right)
        else:
            lrel = left.to_relation(con)
            rrel = right.to_relation(con)
            lschema = _schema_of(lrel)
            rschema = _schema_of(rrel)

        # --- key validation ---
        missing = [k for k in key if k not in lschema or k not in rschema]
        if missing:
            raise DiffError(f"key columns are not present on both sides: {missing}")

        # --- schema delta ---
        schema_changes: list[SchemaChange] = []
        for col, t in lschema.items():
            if col not in rschema:
                schema_changes.append(SchemaChange(col, "removed", old_type=t))
            elif rschema[col] != t:
                schema_changes.append(
                    SchemaChange(col, "type_changed", old_type=t, new_type=rschema[col])
                )
        for col, t in rschema.items():
            if col not in lschema:
                schema_changes.append(SchemaChange(col, "added", new_type=t))

        # --- compared columns: common columns - key - exclude (restricted by columns if given) ---
        common = [c for c in lschema if c in rschema]
        compare_cols = [
            c
            for c in common
            if c not in key
            and c not in exclude_set
            and (columns_filter is None or c in columns_filter)
        ]

        # --- materialize relations + register views ---
        # When projection is active, scan only key + compared columns here (pushdown).
        # Otherwise we already materialized everything above.
        if projection_active:
            proj = key + compare_cols  # compare_cols excludes key, so no duplicates
            lrel = left.to_relation(con, columns=proj)
            rrel = right.to_relation(con, columns=proj)
        lrel.create_view("l", replace=True)
        rrel.create_view("r", replace=True)

        # --- duplicate key ---
        if not allow_duplicates:
            for view, label in (("l", "left"), ("r", "right")):
                if _has_duplicate_keys(con, view, key):
                    raise DiffError(
                        f"{label} has duplicate keys. Use --allow-duplicates to bypass."
                    )

        key_join = " AND ".join(f"l.{_q(k)} IS NOT DISTINCT FROM r.{_q(k)}" for k in key)

        # Build the per-column diff predicate once, shared by counting/streaming (reflects tolerance/ignore_case).
        preds = {
            c: _diff_pred(c, lschema[c], tolerance=tolerance, ignore_case=ignore_case)
            for c in compare_cols
        }

        # --- counts (aggregates first) — actual rows/cells are streamed on access ---
        n_removed = con.execute(
            f"SELECT count(*) FROM l ANTI JOIN r ON {key_join}"
        ).fetchone()[0]
        n_added = con.execute(
            f"SELECT count(*) FROM r ANTI JOIN l ON {key_join}"
        ).fetchone()[0]

        changed_rows = 0
        n_changed_cells = 0
        changed_by_column: list[tuple[str, int]] = []
        if compare_cols:
            any_diff = " OR ".join(f"({preds[c]})" for c in compare_cols)
            # Pull each column's changed-cell count in the same scan (sum totals in Python).
            per_col = ", ".join(
                f"COALESCE(sum(({preds[c]})::INT), 0) AS col{i}"
                for i, c in enumerate(compare_cols)
            )
            row = con.execute(
                f"SELECT count(*) FILTER (WHERE {any_diff}) AS cr, {per_col} "
                f"FROM l JOIN r ON {key_join}"
            ).fetchone()
            changed_rows = int(row[0])
            per_counts = [int(x) for x in row[1:]]
            n_changed_cells = sum(per_counts)
            changed_by_column = sorted(
                ((c, n) for c, n in zip(compare_cols, per_counts) if n > 0),
                key=lambda t: t[1],
                reverse=True,
            )

        removed_sql = f"SELECT l.* FROM l ANTI JOIN r ON {key_join}"
        added_sql = f"SELECT r.* FROM r ANTI JOIN l ON {key_join}"

        return DiffResult(
            key=key,
            schema_changes=schema_changes,
            added=_stream_dicts(con, added_sql),
            removed=_stream_dicts(con, removed_sql),
            changed_cells=_stream_cells(con, key, compare_cols, key_join, preds),
            changed_rows=changed_rows,
            changed_by_column=changed_by_column,
            counts={
                "added": int(n_added),
                "removed": int(n_removed),
                "changed_cells": n_changed_cells,
            },
            resource=con,
        )
    except BaseException:
        # Failure before returning the result -> we close the connection (on success DiffResult owns it).
        con.close()
        raise
