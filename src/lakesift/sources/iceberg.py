"""Iceberg source adapter (v0.3) — reads a snapshot via PyIceberg.

The core/renderers are unchanged. Takes a PyIceberg `Table`, scans it -> Arrow -> hands
it to DuckDB as a relation. The scan currently materializes the whole table into Arrow
(allowed for a single-node tool) — narrow large tables ahead of time with
`row_filter`/`selected_fields`. The diff output itself is still streamed.

pyiceberg is an optional dependency: `pip install "lake-sift[iceberg]"`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from lakesift.sources._deps import require

if TYPE_CHECKING:
    import duckdb
    from pyiceberg.table import Table


class IcebergSource:
    """Reads a PyIceberg `Table` into a DuckDB relation.

    Pass an already-loaded Table directly (`IcebergSource(table)`), or load one from a
    catalog by name (`IcebergSource.from_catalog(...)`).
    """

    def __init__(
        self,
        table: "Table",
        *,
        snapshot_id: int | None = None,
        ref: str | None = None,
        row_filter: Any = None,
        selected_fields: Sequence[str] | None = None,
    ):
        self.table = table
        self.snapshot_id = snapshot_id
        # A named branch or tag (e.g. "staging"). Enables the Write-Audit-Publish
        # pattern: diff a staging branch against main before merging. Mutually
        # exclusive with snapshot_id; ref takes precedence if both are given.
        self.ref = ref
        self.row_filter = row_filter
        self.selected_fields = tuple(selected_fields) if selected_fields else ("*",)

    @classmethod
    def from_catalog(
        cls,
        catalog: str,
        identifier: str,
        *,
        snapshot_id: int | None = None,
        ref: str | None = None,
        row_filter: Any = None,
        selected_fields: Sequence[str] | None = None,
        **properties: Any,
    ) -> "IcebergSource":
        """Load the `identifier` table from a catalog (REST/Glue/SQL/etc.).

        `properties` is passed straight through to pyiceberg `load_catalog` (uri,
        credentials, etc.).
        """
        require("pyiceberg", "iceberg")
        from pyiceberg.catalog import load_catalog

        tbl = load_catalog(catalog, **properties).load_table(identifier)
        return cls(
            tbl,
            snapshot_id=snapshot_id,
            ref=ref,
            row_filter=row_filter,
            selected_fields=selected_fields,
        )

    def arrow_schema(self) -> Any:
        """Return the table schema as an Arrow schema without reading data (metadata)."""
        require("pyiceberg", "iceberg")
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        return schema_to_pyarrow(self.table.schema())

    def to_relation(
        self,
        con: "duckdb.DuckDBPyConnection",
        *,
        columns: Sequence[str] | None = None,
    ) -> "duckdb.DuckDBPyRelation":
        require("pyiceberg", "iceberg")
        # Use the projection passed by the core if any; otherwise the fields set at construction.
        fields = tuple(columns) if columns is not None else self.selected_fields
        kwargs: dict[str, Any] = {"selected_fields": fields}
        # snapshot_id is ignored when a ref is given (use_ref selects the snapshot).
        if self.snapshot_id is not None and self.ref is None:
            kwargs["snapshot_id"] = self.snapshot_id
        if self.row_filter is not None:  # None -> use the scan default (ALWAYS_TRUE)
            kwargs["row_filter"] = self.row_filter
        scan = self.table.scan(**kwargs)
        if self.ref is not None:  # scan a named branch/tag
            scan = scan.use_ref(self.ref)
        arrow = scan.to_arrow()
        return con.from_arrow(arrow)

    def __repr__(self) -> str:  # pragma: no cover
        return f"IcebergSource({self.table!r})"
