"""사람이 보는 컬러 출력 (+ 추가 / - 삭제 / ~ 셀 변경)."""

from __future__ import annotations

from rich.console import Console
from rich.markup import escape

from lakesift.result import DiffResult

# v0: --sample 이 없으므로 콘솔 폭발 방지용 기본 상한.
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
    summary_only: bool = False,
) -> None:
    console = console or Console()

    if result.is_empty():
        console.print("[green]= 차이 없음[/green]")
        return

    # 스키마 델타 (컬럼명/타입은 rich 마크업으로 오해되지 않도록 escape)
    for c in result.schema_changes:
        col = escape(c.column)
        if c.kind == "added":
            console.print(f"[green]+ 컬럼[/green] {col} ({escape(str(c.new_type))})")
        elif c.kind == "removed":
            console.print(f"[red]- 컬럼[/red] {col} ({escape(str(c.old_type))})")
        else:
            console.print(
                f"[yellow]~ 컬럼[/yellow] {col}: "
                f"{escape(str(c.old_type))} → {escape(str(c.new_type))}"
            )

    s = result.summary()
    console.print(
        f"[green]+{s['added']}[/green] 추가  "
        f"[red]-{s['removed']}[/red] 삭제  "
        f"[yellow]~{s['changed']}[/yellow] 변경 행 "
        f"([yellow]{s['changed_cells']}[/yellow] 셀)"
    )

    if summary_only:
        return

    def _emit(items, render, prefix, style):
        for i, it in enumerate(items):
            if i >= max_rows:
                console.print(f"  [dim]... 외 {len(items) - max_rows}건[/dim]")
                break
            console.print(f"[{style}]{prefix}[/{style}] {escape(render(it))}")

    _emit(result.removed, _fmt_row, "-", "red")
    _emit(result.added, _fmt_row, "+", "green")
    _emit(
        result.changed_cells,
        lambda c: f"[{_fmt_key(c.key)}] {c.column}: {c.old!r} → {c.new!r}",
        "~",
        "yellow",
    )
