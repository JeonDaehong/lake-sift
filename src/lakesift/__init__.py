"""lake-sift — value-level data diff (Parquet · Iceberg · Delta, single-node, no Spark)."""

from lakesift.audit import Audit, audit
from lakesift.core import diff, schema_diff
from lakesift.preview import PreviewResult, preview
from lakesift.result import CellChange, DiffResult, SchemaChange
from lakesift.sources.base import DataFileStats
from lakesift.sources.delta import DeltaSource
from lakesift.sources.iceberg import IcebergSource
from lakesift.sources.parquet import ParquetSource
from lakesift.sources.sql import SqlSchemaSource

__all__ = [
    "diff",
    "schema_diff",
    "preview",
    "audit",
    "Audit",
    "DiffResult",
    "PreviewResult",
    "CellChange",
    "SchemaChange",
    "DataFileStats",
    "ParquetSource",
    "IcebergSource",
    "DeltaSource",
    "SqlSchemaSource",
]
__version__ = "0.6.0"
