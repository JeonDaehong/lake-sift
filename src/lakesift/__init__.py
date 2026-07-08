"""lake-sift — value-level data diff (Parquet · Iceberg · Delta, single-node, no Spark)."""

from lakesift.core import diff, schema_diff
from lakesift.result import CellChange, DiffResult, SchemaChange
from lakesift.sources.delta import DeltaSource
from lakesift.sources.iceberg import IcebergSource
from lakesift.sources.parquet import ParquetSource
from lakesift.sources.sql import SqlSchemaSource

__all__ = [
    "diff",
    "schema_diff",
    "DiffResult",
    "CellChange",
    "SchemaChange",
    "ParquetSource",
    "IcebergSource",
    "DeltaSource",
    "SqlSchemaSource",
]
__version__ = "0.5.0"
