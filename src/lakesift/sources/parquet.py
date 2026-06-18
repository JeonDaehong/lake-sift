"""Parquet 소스 어댑터 (v0)."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb


class ParquetSource:
    """단일 Parquet 파일(또는 glob)을 DuckDB relation 으로 읽는다."""

    def __init__(self, path: str | os.PathLike[str]):
        self.path = os.fspath(path)

    def to_relation(self, con: "duckdb.DuckDBPyConnection") -> "duckdb.DuckDBPyRelation":
        # 파라미터 바인딩으로 경로 주입 (SQL 인젝션/따옴표 이슈 회피).
        return con.from_query("SELECT * FROM read_parquet(?)", params=[self.path])

    def __repr__(self) -> str:  # pragma: no cover
        return f"ParquetSource({self.path!r})"
