"""Delta Lake source adapter (v0.4) — reads a table via delta-rs.

The core/renderers are unchanged. Takes a delta-rs `DeltaTable`, materializes it to Arrow
-> hands it to DuckDB as a relation. Isomorphic to the Iceberg adapter (the scan loads the
whole table into Arrow — allowed for a single-node tool; narrow large tables ahead of time
with `columns`/`filters`). The diff output itself is still streamed.

deltalake is an optional dependency: `pip install "lake-sift[delta]"`.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Sequence

if TYPE_CHECKING:
    import duckdb
    from deltalake import DeltaTable


def _require_deltalake() -> None:
    try:
        import deltalake  # noqa: F401
    except ImportError as e:  # pragma: no cover - only in environments without it installed
        raise ImportError(
            'The Delta source requires deltalake: pip install "lake-sift[delta]"'
        ) from e


class DeltaSource:
    """Reads a Delta Lake table into a DuckDB relation via delta-rs.

    Open by path/URI (`DeltaSource("/path/to/table")`, `DeltaSource("s3://bucket/t")`),
    or pass an already-loaded `DeltaTable` directly (`DeltaSource(dt)`). Use `version`
    for time travel to a specific version, and `storage_options` to pass credentials for
    remote storage.
    """

    def __init__(
        self,
        table: "DeltaTable | str | os.PathLike[str]",
        *,
        version: int | None = None,
        storage_options: dict[str, str] | None = None,
        columns: Sequence[str] | None = None,
        filters: Any = None,
    ):
        self.table = table
        self.version = version
        self.storage_options = storage_options
        self.columns = list(columns) if columns else None
        self.filters = filters

    def _load(self) -> "DeltaTable":
        _require_deltalake()
        from deltalake import DeltaTable

        if isinstance(self.table, DeltaTable):
            dt = self.table
            if self.version is not None:  # move the given object to the requested version
                dt.load_as_version(self.version)
            return dt
        return DeltaTable(
            os.fspath(self.table),
            version=self.version,
            storage_options=self.storage_options,
        )

    def arrow_schema(self) -> Any:
        """Return the table schema as an Arrow schema without reading data (metadata)."""
        return self._load().schema().to_arrow()

    def to_relation(
        self,
        con: "duckdb.DuckDBPyConnection",
        *,
        columns: Sequence[str] | None = None,
    ) -> "duckdb.DuckDBPyRelation":
        dt = self._load()
        # Use the projection passed by the core if any; otherwise the columns set at construction.
        cols = list(columns) if columns is not None else self.columns
        arrow = dt.to_pyarrow_table(columns=cols, filters=self.filters)
        return con.from_arrow(arrow)

    def __repr__(self) -> str:  # pragma: no cover
        return f"DeltaSource({self.table!r})"
