"""Parquet 소스 어댑터 (v0)."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Sequence

if TYPE_CHECKING:
    import duckdb


def _q(name: str) -> str:
    """식별자를 안전하게 따옴표로 감싼다."""
    return '"' + name.replace('"', '""') + '"'


class ParquetSource:
    """단일 Parquet 파일(또는 glob)을 DuckDB relation 으로 읽는다."""

    def __init__(self, path: str | os.PathLike[str]):
        self.path = os.fspath(path)

    def to_relation(
        self,
        con: "duckdb.DuckDBPyConnection",
        *,
        columns: Sequence[str] | None = None,
    ) -> "duckdb.DuckDBPyRelation":
        # 파라미터 바인딩으로 경로 주입 (SQL 인젝션/따옴표 이슈 회피).
        select = "*" if columns is None else ", ".join(_q(c) for c in columns)
        return con.from_query(f"SELECT {select} FROM read_parquet(?)", params=[self.path])

    def __repr__(self) -> str:  # pragma: no cover
        return f"ParquetSource({self.path!r})"
