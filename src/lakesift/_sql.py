"""Small SQL helpers shared across the core and source adapters."""

from __future__ import annotations


def quote_identifier(name: str) -> str:
    """Safely quote a SQL identifier (column/table name) for DuckDB."""
    return '"' + name.replace('"', '""') + '"'
