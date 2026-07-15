"""Iceberg source adapter (v0.3) — reads a snapshot via PyIceberg.

The core/renderers are unchanged. Takes a PyIceberg `Table`, scans it -> Arrow -> hands
it to DuckDB as a relation. The scan currently materializes the whole table into Arrow
(allowed for a single-node tool) — narrow large tables ahead of time with
`row_filter`/`selected_fields`. The diff output itself is still streamed.

pyiceberg is an optional dependency: `pip install "lake-sift[iceberg]"`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from lakesift.core import DiffError
from lakesift.sources._deps import require
from lakesift.sources.base import DataFileStats

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

    def _scan(self, fields: Sequence[str]) -> Any:
        """Build the configured scan (snapshot/ref, row filter, projection)."""
        kwargs: dict[str, Any] = {"selected_fields": tuple(fields)}
        # snapshot_id is ignored when a ref is given (use_ref selects the snapshot).
        if self.snapshot_id is not None and self.ref is None:
            kwargs["snapshot_id"] = self.snapshot_id
        if self.row_filter is not None:  # None -> use the scan default (ALWAYS_TRUE)
            kwargs["row_filter"] = self.row_filter
        scan = self.table.scan(**kwargs)
        if self.ref is not None:  # scan a named branch/tag
            scan = scan.use_ref(self.ref)
        return scan

    def to_relation(
        self,
        con: "duckdb.DuckDBPyConnection",
        *,
        columns: Sequence[str] | None = None,
    ) -> "duckdb.DuckDBPyRelation":
        require("pyiceberg", "iceberg")
        # Use the projection passed by the core if any; otherwise the fields set at construction.
        fields = tuple(columns) if columns is not None else self.selected_fields
        return con.from_arrow(self._scan(fields).to_arrow())

    def data_files(self) -> list[DataFileStats]:
        """Describe the scanned snapshot's data files from the manifests — no data read.

        Iceberg records, per data file, its path, row count, size, partition values and
        per-column bounds; planning a scan reads only those manifests. That is enough for
        `preview()` to reason about what changed between two snapshots without opening a
        single Parquet file.

        Bounds are stored keyed by field id and encoded as bytes, so they are decoded here
        against the table schema. A field we cannot resolve or decode is simply omitted —
        a missing bound means "unknown", which callers already treat conservatively.

        Raises:
            DiffError: a `row_filter` is set. Manifests count *whole files*, and planning
                only prunes files that cannot match — it cannot say how many rows inside a
                surviving file pass the filter. Every count here would then be an
                overcount, which silently breaks the guarantee that `preview()`'s
                added/removed figures are lower bounds on the real diff.
        """
        require("pyiceberg", "iceberg")
        from pyiceberg.conversions import from_bytes

        if self.row_filter is not None:
            raise DiffError(
                "preview cannot bound a filtered scan: Iceberg manifests count whole "
                "files, not the rows inside them that pass a row_filter. Preview the "
                "unfiltered source, or run the full diff."
            )

        schema = self.table.schema()
        types = {f.field_id: f.field_type for f in schema.fields}
        names = {f.field_id: f.name for f in schema.fields}
        specs = self.table.specs()

        def decode(raw_bounds: Any) -> dict[str, Any]:
            out: dict[str, Any] = {}
            for fid, raw in (raw_bounds or {}).items():
                if fid not in types:  # nested/dropped field — no top-level column to key it by
                    continue
                try:
                    out[names[fid]] = from_bytes(types[fid], raw)
                except Exception:
                    continue  # undecodable bound == no bound
            return out

        def partition_of(file: Any) -> dict[str, Any]:
            """Partition values as {field name: value} (empty for unpartitioned tables)."""
            spec = specs.get(file.spec_id)
            if spec is None:
                return {}
            return {pf.name: file.partition[i] for i, pf in enumerate(spec.fields)}

        # Two sources over the same files still yield different rows if they project
        # different columns, so the projection discriminates a file's identity.
        scope = ",".join(self.selected_fields)

        out: list[DataFileStats] = []
        for task in self._scan(("*",)).plan_files():
            f = task.file
            out.append(
                DataFileStats(
                    path=f.file_path,
                    record_count=f.record_count,
                    size_bytes=f.file_size_in_bytes,
                    partition=partition_of(f),
                    lower_bounds=decode(f.lower_bounds),
                    upper_bounds=decode(f.upper_bounds),
                    delete_files=frozenset(d.file_path for d in task.delete_files),
                    scope=scope,
                )
            )
        return out

    def __repr__(self) -> str:  # pragma: no cover
        return f"IcebergSource({self.table!r})"
