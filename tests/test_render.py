"""human 렌더러 출력 검증."""

from __future__ import annotations

from rich.console import Console

from lakesift.render.human import render_human
from lakesift.result import CellChange, DiffResult


def _render(result: DiffResult) -> str:
    # 색상/마크업 없이 순수 텍스트만 캡처
    console = Console(record=True, no_color=True, width=200, markup=True)
    render_human(result, console=console)
    return console.export_text()


def test_changed_cell_shows_key():
    """[id=3] 같은 key 표기가 rich 마크업으로 먹히지 않고 그대로 보여야 한다."""
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
    assert "차이 없음" in out
