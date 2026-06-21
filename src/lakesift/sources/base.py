"""Source interface — resolves an input into a (DuckDB relation + schema).

Swap the adapter and you add an input format. The core/renderers stay the same.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, Sequence, runtime_checkable

if TYPE_CHECKING:
    import duckdb


@runtime_checkable
class Source(Protocol):
    """The minimal contract every input source must implement.

    Optionally implementing `arrow_schema() -> pyarrow.Schema` lets the core fetch the
    schema cheaply, without reading data, before deciding on column projection
    (pushdown) — useful for sources like Iceberg/Delta where `to_relation` materializes
    everything. If not implemented, the core reads the schema from the `to_relation`
    relation.
    """

    def to_relation(
        self,
        con: "duckdb.DuckDBPyConnection",
        *,
        columns: Sequence[str] | None = None,
    ) -> "duckdb.DuckDBPyRelation":
        """Turn this source into a DuckDB relation.

        If `columns` is given, read only those columns (pushed down to the scan).
        If `None`, read everything.
        """
        ...
