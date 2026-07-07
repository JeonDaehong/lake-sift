"""CLI (typer) — a thin wrapper over the library API `diff()`.

Exit-code convention: 0 = identical, 1 = differences found, 2 = error (comparison
not possible).
"""

from __future__ import annotations

import sys
from typing import Optional

import typer
from rich.console import Console

from lakesift import __version__
from lakesift.core import DiffError, diff
from lakesift.render.human import render_human
from lakesift.render.markdown import render_markdown
from lakesift.sources.base import Source
from lakesift.sources.delta import DeltaSource
from lakesift.sources.iceberg import IcebergSource
from lakesift.sources.parquet import ParquetSource

app = typer.Typer(add_completion=False, help="Diff two datasets at the cell level (Parquet · Iceberg · Delta).")

_ICEBERG_PREFIX = "iceberg:"
_DELTA_PREFIX = "delta:"


def _split(value: Optional[str]) -> Optional[list[str]]:
    if not value:
        return None
    return [c.strip() for c in value.split(",") if c.strip()]


def _source(spec: str) -> Source:
    """Resolve an operand string into a Source.

    - `iceberg:<catalog>/<namespace>.<table>[@<snapshot_id>]` -> IcebergSource
      (catalog connection details are read from PyIceberg's standard config,
      `~/.pyiceberg.yaml`/env)
    - `delta:<path-or-uri>[@<version>]` -> DeltaSource (local path or a URI such as s3://)
    - anything else -> ParquetSource (file path or glob)
    """
    if spec.startswith(_ICEBERG_PREFIX):
        return _iceberg_source(spec[len(_ICEBERG_PREFIX):])
    if spec.startswith(_DELTA_PREFIX):
        return _delta_source(spec[len(_DELTA_PREFIX):])
    return ParquetSource(spec)


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
    console = Console()
    err = Console(stderr=True)

    keys = _split(key)
    if not keys:
        err.print("[red]error:[/red] --key is required.")
        raise typer.Exit(code=2)

    try:
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

    # result owns a live connection — close it for sure with `with`.
    with result:
        if json_out:
            # Stream rows/cells one at a time (bypassing rich buffering/coloring).
            result.write_json(sys.stdout)
            sys.stdout.write("\n")
        elif markdown:
            kw = {} if sample is None else {"max_rows": sample}
            sys.stdout.write(
                render_markdown(result, summary_only=summary, top_columns=top, **kw)
            )
        else:
            kw = {} if sample is None else {"max_rows": sample}
            render_human(
                result, console=console, summary_only=summary, top_columns=top, **kw
            )
        empty = result.is_empty()

    raise typer.Exit(code=0 if empty else 1)


if __name__ == "__main__":  # pragma: no cover
    app()
