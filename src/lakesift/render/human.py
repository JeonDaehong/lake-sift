"""Human-readable color output (+ added / - removed / ~ changed cell)."""

from __future__ import annotations

from rich.console import Console
from rich.markup import escape

from lakesift.result import DiffResult

# v0: with no --sample, a default cap to keep the console from exploding.
DEFAULT_MAX_ROWS = 20


def _fmt_key(key: dict) -> str:
    return ", ".join(f"{k}={v!r}" for k, v in key.items())


def _fmt_row(row: dict) -> str:
    return ", ".join(f"{k}={v!r}" for k, v in row.items())


def render_human(
    result: DiffResult,
    *,
    console: Console | None = None,
    max_rows: int = DEFAULT_MAX_ROWS,
    top_columns: int = 5,
    summary_only: bool = False,
) -> None:
    console = console or Console()

    if result.is_empty():
        console.print("[green]= no differences[/green]")
        return

    # Schema deltas (escape column names/types so they aren't read as rich markup).
    for c in result.schema_changes:
        col = escape(c.column)
        if c.kind == "added":
            console.print(f"[green]+ column[/green] {col} ({escape(str(c.new_type))})")
        elif c.kind == "removed":
            console.print(f"[red]- column[/red] {col} ({escape(str(c.old_type))})")
        else:
            console.print(
                f"[yellow]~ column[/yellow] {col}: "
                f"{escape(str(c.old_type))} → {escape(str(c.new_type))}"
            )

    s = result.summary()
    console.print(
        f"[green]+{s['added']}[/green] added  "
        f"[red]-{s['removed']}[/red] removed  "
        f"[yellow]~{s['changed']}[/yellow] changed rows "
        f"([yellow]{s['changed_cells']}[/yellow] cells)"
    )

    # Top-K columns by changed cells (to see at a glance where things shifted).
    if top_columns > 0 and result.changed_by_column:
        top = result.changed_by_column[:top_columns]
        parts = ", ".join(f"{escape(col)} ({n})" for col, n in top)
        rest = len(result.changed_by_column) - len(top)
        if rest > 0:
            parts += f", [dim]… +{rest} more[/dim]"
        console.print(f"  [dim]top changed columns:[/dim] {parts}")

    if summary_only:
        return

    def _emit(items, total, render, prefix, style):
        # items may be a streaming iterator — don't materialize, stop at max_rows.
        shown = 0
        for it in items:
            if shown >= max_rows:
                console.print(f"  [dim]... +{total - max_rows} more[/dim]")
                break
            console.print(f"[{style}]{prefix}[/{style}] {escape(render(it))}")
            shown += 1

    _emit(result.removed, s["removed"], _fmt_row, "-", "red")
    _emit(result.added, s["added"], _fmt_row, "+", "green")
    _emit(
        result.changed_cells,
        s["changed_cells"],
        lambda c: f"[{_fmt_key(c.key)}] {c.column}: {c.old!r} → {c.new!r}",
        "~",
        "yellow",
    )
