"""Human-readable color output (+ added / - removed / ~ changed cell)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from rich.console import Console
from rich.markup import escape

from lakesift.render._shared import (
    DEFAULT_MAX_ROWS,
    fmt_pairs as _fmt_pairs,
    preview_bounds,
    preview_facts,
    sampled,
    schema_detail,
    top_split,
)
from lakesift.result import DiffResult

if TYPE_CHECKING:
    from lakesift.preview import PreviewResult

# Per-kind (color, symbol) for a schema-change line; the shared type trailer follows.
_SCHEMA_STYLE = {"added": ("green", "+"), "removed": ("red", "-"), "type_changed": ("yellow", "~")}


def _schema_lines(result, console: Console) -> None:
    """Print the schema deltas (escaped so names/types aren't read as rich markup)."""
    for c in result.schema_changes:
        color, sym = _SCHEMA_STYLE[c.kind]
        console.print(
            f"[{color}]{sym} column[/{color}] {escape(c.column)}{escape(schema_detail(c))}"
        )


def render_preview_human(preview: "PreviewResult", *, console: Console | None = None) -> None:
    """Print a metadata-only preview: what a diff would cost and what it can't be."""
    console = console or Console()

    # Non-ASCII punctuation is avoided in console output: a Windows cp949 console cannot
    # encode an em dash and would crash. ('→' and '…' are in cp949 and are safe.)
    if preview.is_empty():
        console.print(
            "[green]= provably identical[/green] [dim](every data file is shared; no data read)[/dim]"
        )
        return

    _schema_lines(preview, console)

    def rows(facts) -> None:
        for label, value, note in facts:
            trailer = f"  [dim]{escape(note)}[/dim]" if note else ""
            console.print(f"  [dim]{label:<17}[/dim]{escape(value)}{trailer}")

    console.print("[bold]blast radius[/bold] [dim](from metadata only, no data read)[/dim]")
    rows(preview_facts(preview))

    bounds = preview_bounds(preview)
    if bounds:
        console.print("[bold]proof[/bold] [dim](from key ranges)[/dim]")
        rows(bounds)
    else:
        console.print("  [dim]pass --key for the key-range proofs[/dim]")

    if preview.has_deletes:
        console.print("  [dim]note: merge-on-read delete files present[/dim]")


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

    _schema_lines(result, console)

    s = result.summary()
    console.print(
        f"[green]+{s['added']}[/green] added  "
        f"[red]-{s['removed']}[/red] removed  "
        f"[yellow]~{s['changed']}[/yellow] changed rows "
        f"([yellow]{s['changed_cells']}[/yellow] cells)"
    )

    # Top-K columns by changed cells (to see at a glance where things shifted).
    if top_columns > 0 and result.changed_by_column:
        top, rest = top_split(result.changed_by_column, top_columns)
        parts = ", ".join(f"{escape(col)} ({n})" for col, n in top)
        if rest > 0:
            parts += f", [dim]… +{rest} more[/dim]"
        console.print(f"  [dim]top changed columns:[/dim] {parts}")

    if summary_only:
        return

    def _emit(items, total, render, prefix, style):
        # items may be a streaming iterator — sampled() caps it without materializing.
        for kind, val in sampled(items, total, max_rows):
            if kind == "more":
                console.print(f"  [dim]... +{val} more[/dim]")
            else:
                console.print(f"[{style}]{prefix}[/{style}] {escape(render(val))}")

    _emit(result.removed, s["removed"], _fmt_pairs, "-", "red")
    _emit(result.added, s["added"], _fmt_pairs, "+", "green")
    _emit(
        result.changed_cells,
        s["changed_cells"],
        lambda c: f"[{_fmt_pairs(c.key)}] {c.column}: {c.old!r} → {c.new!r}",
        "~",
        "yellow",
    )
