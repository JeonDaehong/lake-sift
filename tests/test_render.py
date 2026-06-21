"""human renderer output."""

from __future__ import annotations

from rich.console import Console

from lakesift.render.human import render_human
from lakesift.result import CellChange, DiffResult


def _render(result: DiffResult) -> str:
    # capture plain text only, no color/markup
    console = Console(record=True, no_color=True, width=200, markup=True)
    render_human(result, console=console)
    return console.export_text()


def test_changed_cell_shows_key():
    """A key like [id=3] must be shown verbatim, not eaten by rich markup."""
    result = DiffResult(
        key=["id"],
        changed_cells=[CellChange(key={"id": 3}, column="name", old="c", new="C")],
        changed_rows=1,
    )
    out = _render(result)
    assert "[id=3]" in out
    assert "name" in out and "'c'" in out and "'C'" in out


def test_empty_result_message():
    out = _render(DiffResult(key=["id"]))
    assert "no differences" in out
