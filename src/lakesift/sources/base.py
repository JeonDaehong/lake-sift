"""Source interface — resolves an input into a (DuckDB relation + schema).

Swap the adapter and you add an input format. The core/renderers stay the same.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, Sequence, runtime_checkable

if TYPE_CHECKING:
    import duckdb


@dataclass(frozen=True)
class DataFileStats:
    """One immutable data file, as described by the table's own metadata.

    This is the unit `preview()` reasons over. Formats with immutable data files and
    per-file statistics (Iceberg, Delta) can describe a table this way without opening a
    single file, which is what makes a metadata-only preview possible.

    `lower_bounds`/`upper_bounds` are column name -> value, and must be *sound*: the real
    values in the file are within the bounds. (Iceberg truncates long string bounds
    outward — down for lower, up for upper — precisely to preserve this.) A column absent
    from either dict simply has no bound, and callers must then assume any value.
    """

    path: str
    record_count: int
    size_bytes: int
    partition: dict[str, Any] = field(default_factory=dict)
    lower_bounds: dict[str, Any] = field(default_factory=dict)
    upper_bounds: dict[str, Any] = field(default_factory=dict)
    # Delete files applied to this data file (merge-on-read). Part of the file's identity:
    # the same data file with a different delete set yields different rows.
    delete_files: frozenset[str] = frozenset()
    # How the source reads this file (its projection, etc). Also part of the identity: the
    # same bytes read through a different projection do not yield the same rows, so a
    # source must set this to anything that changes what a read returns.
    scope: str = ""

    @property
    def identity(self) -> tuple[str, frozenset[str], str]:
        """What makes this file's *contribution* unique.

        Two sides sharing an identity are yielding identical rows — data files are
        immutable, so the path pins the bytes, the delete set pins which of them survive,
        and the scope pins how they are read. This is the fact the whole preview is built
        on, so anything that changes the rows a file contributes belongs here: an identity
        that ignores such a difference would let `preview()` call two sides "provably
        identical" when they are not.
        """
        return (self.path, self.delete_files, self.scope)


@runtime_checkable
class Source(Protocol):
    """The minimal contract every input source must implement.

    Optionally implementing `arrow_schema() -> pyarrow.Schema` lets the core fetch the
    schema cheaply, without reading data, before deciding on column projection
    (pushdown) — useful for sources like Iceberg/Delta where `to_relation` materializes
    everything. If not implemented, the core reads the schema from the `to_relation`
    relation.

    Optionally implementing `data_files() -> Sequence[DataFileStats]` (describing the
    source from metadata alone) makes the source eligible for `preview()`.
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
