"""CLI (typer) — 라이브러리 API `diff()` 의 얇은 래퍼.

exit code 규약: 0 = 동일, 1 = 차이 있음, 2 = 에러(비교 불가).
"""

from __future__ import annotations

import sys
from typing import Optional

import typer
from rich.console import Console

from lakesift.core import DiffError, diff
from lakesift.render.human import render_human
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
    tolerance: Optional[float] = typer.Option(
        None, "--tolerance", "-t", help="수치 컬럼 허용 오차 (abs(l-r)<=tol 이면 같음)"
    ),
    ignore_case: bool = typer.Option(
        False, "--ignore-case", "-i", help="문자열 컬럼 대소문자 무시 비교"
    ),
    sample: Optional[int] = typer.Option(
        None, "--sample", "-n", help="사람용 출력에서 종류별 표시할 최대 건수"
    ),
    top: int = typer.Option(
        5, "--top", help="변경 셀 상위 컬럼 K개 표시 (0=끄기)"
    ),
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
            tolerance=tolerance,
            ignore_case=ignore_case,
        )
    except DiffError as e:
        err.print(f"[red]에러:[/red] {e}")
        raise typer.Exit(code=2)
    except Exception as e:  # 읽기 실패 등도 비교 불가로 취급
        err.print(f"[red]에러:[/red] {e}")
        raise typer.Exit(code=2)

    # result 는 살아있는 커넥션을 소유한다 — with 로 확실히 닫는다.
    with result:
        if json_out:
            # 행/셀을 한 건씩 흘려쓴다 (rich 버퍼링/색 입히기 우회).
            result.write_json(sys.stdout)
            sys.stdout.write("\n")
        else:
            kw = {} if sample is None else {"max_rows": sample}
            render_human(
                result, console=console, summary_only=summary, top_columns=top, **kw
            )
        empty = result.is_empty()

    raise typer.Exit(code=0 if empty else 1)


if __name__ == "__main__":  # pragma: no cover
    app()
