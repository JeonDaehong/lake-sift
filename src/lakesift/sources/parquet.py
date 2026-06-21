"""Parquet source adapter (v0)."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Sequence

if TYPE_CHECKING:
    import duckdb


def _q(name: str) -> str:
    """Safely quote an identifier."""
    return '"' + name.replace('"', '""') + '"'


class ParquetSource:
    """Reads a single Parquet file (or glob) into a DuckDB relation."""

    def __init__(self, path: str | os.PathLike[str]):
        self.path = os.fspath(path)

    def to_relation(
        self,
        con: "duckdb.DuckDBPyConnection",
        *,
        columns: Sequence[str] | None = None,
    ) -> "duckdb.DuckDBPyRelation":
        # Inject the path via parameter binding (avoids SQL injection/quoting issues).
        select = "*" if columns is None else ", ".join(_q(c) for c in columns)
        return con.from_query(f"SELECT {select} FROM read_parquet(?)", params=[self.path])

    def __repr__(self) -> str:  # pragma: no cover
        return f"ParquetSource({self.path!r})"
