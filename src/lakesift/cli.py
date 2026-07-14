"""CLI (typer) — a thin wrapper over the library API `diff()`.

Exit-code convention: 0 = identical, 1 = differences found, 2 = error (comparison
not possible).
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from lakesift import __version__
from lakesift.core import DiffError, diff, schema_diff
from lakesift.render.human import render_human
from lakesift.render.markdown import render_markdown
from lakesift.sources.base import Source
from lakesift.sources.delta import DeltaSource
from lakesift.sources.iceberg import IcebergSource
from lakesift.sources.parquet import ParquetSource
from lakesift.sources.sql import SqlSchemaSource

app = typer.Typer(add_completion=False, help="Diff two datasets at the cell level (Parquet · Iceberg · Delta).")

_ICEBERG_PREFIX = "iceberg:"
_DELTA_PREFIX = "delta:"
_SQL_PREFIX = "sql:"


def _split(value: Optional[str]) -> Optional[list[str]]:
    if not value:
        return None
    return [c.strip() for c in value.split(",") if c.strip()]


def _source(spec: str, *, upstreams: Optional[dict[str, str]] = None, dialect: str = "duckdb") -> Source:
    """Resolve an operand string into a Source.

    - `iceberg:<catalog>/<namespace>.<table>[@<snapshot_id>]` -> IcebergSource
      (catalog connection details are read from PyIceberg's standard config,
      `~/.pyiceberg.yaml`/env)
    - `delta:<path-or-uri>[@<version>]` -> DeltaSource (local path or a URI such as s3://)
    - `sql:<path-to-.sql-file>` -> SqlSchemaSource (predicted output schema; needs
      `--upstream NAME=SOURCE` for each table the query reads; --schema-only only)
    - anything else -> ParquetSource (file path or glob)
    """
    if spec.startswith(_ICEBERG_PREFIX):
        return _iceberg_source(spec[len(_ICEBERG_PREFIX):])
    if spec.startswith(_DELTA_PREFIX):
        return _delta_source(spec[len(_DELTA_PREFIX):])
    if spec.startswith(_SQL_PREFIX):
        return _sql_source(spec[len(_SQL_PREFIX):], upstreams or {}, dialect)
    return ParquetSource(spec)


def _parse_upstreams(items: Optional[list[str]]) -> dict[str, str]:
    """`--upstream NAME=SOURCE` entries -> {name: source-spec}. SOURCE is itself an
    operand spec (Parquet path, iceberg:…, delta:…)."""
    out: dict[str, str] = {}
    for item in items or []:
        name, sep, spec = item.partition("=")
        name, spec = name.strip(), spec.strip()
        if not sep or not name or not spec:
            raise DiffError(f"--upstream must be NAME=SOURCE: {item!r}")
        out[name] = spec
    return out


def _sql_source(rest: str, upstreams: dict[str, str], dialect: str) -> SqlSchemaSource:
    if not rest:
        raise DiffError("SQL source format: sql:<path-to-.sql-file>")
    if not upstreams:
        raise DiffError(
            "sql: source needs at least one --upstream NAME=SOURCE (the query's input tables)."
        )
    try:
        sql_text = Path(rest).read_text(encoding="utf-8")
    except OSError as e:
        raise DiffError(f"cannot read SQL file {rest!r}: {e}")
    resolved = {name: _source(spec) for name, spec in upstreams.items()}
    return SqlSchemaSource(sql_text, resolved, dialect=dialect)


def _split_ref(rest: str, what: str) -> tuple[str, str | None]:
    """Split a `body@token` operand on the last '@'.

    Returns `(body, token)`, or `(rest, None)` when there is no '@'. A trailing '@'
    with nothing after it (empty token) is a usage error.
    """
    if "@" not in rest:
        return rest, None
    body, _, token = rest.rpartition("@")
    if not token:
        raise DiffError(f"{what} after '@' is empty.")
    return body, token


def _iceberg_source(rest: str) -> IcebergSource:
    # After '@': an integer is a snapshot id, anything else is a branch/tag ref name.
    rest, token = _split_ref(rest, "Iceberg ref/snapshot")
    snapshot_id: int | None = None
    ref: str | None = None
    if token is not None:
        try:
            snapshot_id = int(token)
        except ValueError:
            ref = token  # non-integer -> a branch or tag name
    catalog, sep, identifier = rest.partition("/")
    if not sep or not catalog or not identifier:
        raise DiffError(
            "Iceberg source format: iceberg:<catalog>/<namespace>.<table>[@<snapshot_id-or-ref>]"
        )
    return IcebergSource.from_catalog(catalog, identifier, snapshot_id=snapshot_id, ref=ref)


def _delta_source(rest: str) -> DeltaSource:
    rest, token = _split_ref(rest, "Delta version")
    version: int | None = None
    if token is not None:
        try:
            version = int(token)
        except ValueError:
            raise DiffError(f"Delta version must be an integer: {token!r}")
    if not rest:
        raise DiffError("Delta source format: delta:<path-or-uri>[@<version>]")
    return DeltaSource(rest, version=version)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"lake-sift {__version__}")
        raise typer.Exit()


@app.command()
def main(
    left: str = typer.Argument(
        ...,
        help="Base (before) source — Parquet path · iceberg:<catalog>/<ns>.<table>[@<snap|ref>] · delta:<path>[@<ver>]",
    ),
    right: str = typer.Argument(
        ...,
        help="Compared (after) source — Parquet path · iceberg:<catalog>/<ns>.<table>[@<snap|ref>] · delta:<path>[@<ver>]",
    ),
    key: Optional[str] = typer.Option(None, "--key", "-k", help="Row identity key (comma-separated)"),
    exclude: Optional[str] = typer.Option(None, "--exclude", "-x", help="Columns to exclude from comparison"),
    columns: Optional[str] = typer.Option(None, "--columns", "-c", help="Compare only these columns"),
    json_out: bool = typer.Option(False, "--json", help="Machine-readable JSON output"),
    markdown: bool = typer.Option(
        False, "--markdown", help="Markdown report (for PR comments / CI step summaries)"
    ),
    summary: bool = typer.Option(False, "--summary", help="Print the summary only"),
    schema_only: bool = typer.Option(
        False,
        "--schema-only",
        help="Compare only schemas, reading no data (no --key needed). A pre-execution / "
        "contract gate: catch added/removed/retyped columns without materializing rows.",
    ),
    structural_only: bool = typer.Option(
        False,
        "--structural-only",
        help="With --schema-only, compare column presence only (ignore type changes) — "
        "appropriate when one side's types are best-effort predictions (a sql: source).",
    ),
    upstream: Optional[list[str]] = typer.Option(
        None,
        "--upstream",
        "-u",
        help="NAME=SOURCE input table for a sql: operand (repeatable); SOURCE is a "
        "Parquet path / iceberg: / delta: spec.",
    ),
    sql_dialect: str = typer.Option(
        "duckdb", "--sql-dialect", help="SQL dialect for a sql: operand (SQLGlot dialect name)."
    ),
    allow_duplicates: bool = typer.Option(False, "--allow-duplicates", help="Allow duplicate keys"),
    tolerance: Optional[float] = typer.Option(
        None, "--tolerance", "-t", help="Numeric tolerance (equal when abs(l-r) <= tol)"
    ),
    ignore_case: bool = typer.Option(
        False, "--ignore-case", "-i", help="Case-insensitive comparison for text columns"
    ),
    sample: Optional[int] = typer.Option(
        None, "--sample", "-n", min=0, help="Max rows to show per change kind in human output"
    ),
    top: int = typer.Option(
        5, "--top", min=0, help="Show the top K columns by changed cells (0 = off)"
    ),
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        callback=_version_callback,
        is_eager=True,
        help="Show the version and exit.",
    ),
) -> None:
    """Diff two datasets at the cell level (Parquet, Iceberg, Delta).

    For a reproducible diff, pin an immutable point in time on each side: a Parquet
    file, an Iceberg snapshot id (@1042), or a Delta version (@12). A moving Iceberg
    branch/tag (@main) advances with concurrent writes, so reserve it for the
    isolated staging branch in Write-Audit-Publish.
    """
    console = Console()
    err = Console(stderr=True)

    keys = _split(key)
    if not schema_only and not keys:
        err.print("[red]error:[/red] --key is required (unless --schema-only).")
        raise typer.Exit(code=2)

    try:
        upstreams = _parse_upstreams(upstream)
        if schema_only:
            # Schema-only: no key, no data read, other comparison options don't apply.
            result = schema_diff(
                _source(left, upstreams=upstreams, dialect=sql_dialect),
                _source(right, upstreams=upstreams, dialect=sql_dialect),
                compare_types=not structural_only,
            )
        else:
            result = diff(
                left=_source(left),
                right=_source(right),
                key=keys,
                exclude=_split(exclude),
                columns=_split(columns),
                allow_duplicates=allow_duplicates,
                tolerance=tolerance,
                ignore_case=ignore_case,
            )
    except Exception as e:
        # DiffError is the expected case; any other exception (read failures, etc.) is
        # also treated as comparison-not-possible and mapped to exit code 2.
        err.print(f"[red]error:[/red] {e}")
        raise typer.Exit(code=2)

    # Only override the renderer's default row cap when --sample was given.
    row_cap = {} if sample is None else {"max_rows": sample}

    # result owns a live connection — close it for sure with `with`.
    with result:
        if json_out:
            # Stream rows/cells one at a time (bypassing rich buffering/coloring).
            result.write_json(sys.stdout)
            sys.stdout.write("\n")
        elif markdown:
            sys.stdout.write(
                render_markdown(result, summary_only=summary, top_columns=top, **row_cap)
            )
        else:
            render_human(
                result, console=console, summary_only=summary, top_columns=top, **row_cap
            )
        empty = result.is_empty()

    raise typer.Exit(code=0 if empty else 1)


if __name__ == "__main__":  # pragma: no cover
    app()
