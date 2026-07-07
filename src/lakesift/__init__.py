"""lake-sift — value-level data diff (Parquet · Iceberg · Delta, single-node, no Spark)."""

from lakesift.core import diff
from lakesift.result import CellChange, DiffResult, SchemaChange
from lakesift.sources.delta import DeltaSource
from lakesift.sources.iceberg import IcebergSource
from lakesift.sources.parquet import ParquetSource

__all__ = [
    "diff",
    "DiffResult",
    "CellChange",
    "SchemaChange",
    "ParquetSource",
    "IcebergSource",
    "DeltaSource",
]
__version__ = "0.5.0"
