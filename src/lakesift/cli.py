"""CLI (typer) — 라이브러리 API `diff()` 의 얇은 래퍼.

exit code 규약: 0 = 동일, 1 = 차이 있음, 2 = 에러(비교 불가).
"""

from __future__ import annotations

from typing import Optional

import typer
from rich.console import Console

from lakesift.core import DiffError, diff
from lakesift.render.human import render_human
from lakesift.render.json_render import render_json
from lakesift.sources.parquet import ParquetSource

app = typer.Typer(add_completion=False, help="두 Parquet 파일을 셀 단위로 diff 한다.")


def _split(value: Optional[str]) -> Optional[list[str]]:
    if not value:
        return None
    return [c.strip() for c in value.split(",") if c.strip()]


@app.command()
def main(
    left: str = typer.Argument(..., help="기준(이전) Parquet 경로"),
    right: str = typer.Argument(..., help="비교(이후) Parquet 경로"),
    key: Optional[str] = typer.Option(None, "--key", "-k", help="행 식별 key (쉼표 구분)"),
    exclude: Optional[str] = typer.Option(None, "--exclude", "-x", help="비교 제외 컬럼"),
    columns: Optional[str] = typer.Option(None, "--columns", "-c", help="이 컬럼만 비교"),
    json_out: bool = typer.Option(False, "--json", help="기계용 JSON 출력"),
    summary: bool = typer.Option(False, "--summary", help="요약만 출력"),
    allow_duplicates: bool = typer.Option(False, "--allow-duplicates", help="중복 key 허용"),
) -> None:
    console = Console()
    err = Console(stderr=True)

    keys = _split(key)
    if not keys:
        err.print("[red]에러:[/red] --key 가 필요합니다.")
        raise typer.Exit(code=2)

    try:
        result = diff(
            left=ParquetSource(left),
            right=ParquetSource(right),
            key=keys,
            exclude=_split(exclude),
            columns=_split(columns),
            allow_duplicates=allow_duplicates,
        )
    except DiffError as e:
        err.print(f"[red]에러:[/red] {e}")
        raise typer.Exit(code=2)
    except Exception as e:  # 읽기 실패 등도 비교 불가로 취급
        err.print(f"[red]에러:[/red] {e}")
        raise typer.Exit(code=2)

    if json_out:
        console.print_json(render_json(result))
    else:
        render_human(result, console=console, summary_only=summary)

    raise typer.Exit(code=0 if result.is_empty() else 1)


if __name__ == "__main__":  # pragma: no cover
    app()
