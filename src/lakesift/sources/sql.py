"""SQL schema-prediction source (Step B) — infers a query's *output schema* statically.

Given a SQL query plus the schemas of its upstream tables, SQLGlot derives the output
column list and (best-effort) types **without executing the query or reading any data**.
The upstream schemas are taken from ordinary sources (their cheap schema probe), so the
whole prediction touches metadata only — nothing hits a warehouse or scans a file.

This is schema-only by nature: it implements `arrow_schema()` but not a materializable
`to_relation()`, so it is meant for `schema_diff()`, not `diff()`. Pair the predicted
schema with the live table's schema to gate a pipeline change *before* it runs — the
lakehouse analogue of a reasoner checking an ontology before use.

Reliability: **structural** prediction (which columns exist / are dropped / renamed) is
trustworthy; **type** prediction is best-effort (decimal scale, int width, unknown
functions/UDFs may drift from what the engine ultimately produces). For a purely
structural gate, compare with `schema_diff(..., compare_types=False)`. Output columns
whose type SQLGlot cannot infer (e.g. an unknown UDF) are reported with a null type.

sqlglot is an optional dependency: `pip install "lake-sift[sql]"`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping, Union

import pyarrow as pa

from lakesift.core import DiffError, _probe_schema, _schema_of
from lakesift.sources._deps import require

if TYPE_CHECKING:
    import duckdb

    from lakesift.sources.base import Source

# An upstream may be given as a real Source (schema probed from it) or a ready pyarrow.Schema.
Upstream = Union["Source", pa.Schema]


class SqlSchemaSource:
    """Predicts the output schema of `sql` from its `upstreams`, without running it.

    `upstreams` maps each table name referenced in the query to either a `Source`
    (whose schema is probed cheaply) or a `pyarrow.Schema` directly::

        pred = SqlSchemaSource(
            "SELECT id, amount FROM orders WHERE status = 'paid'",
            upstreams={"orders": IcebergSource.from_catalog("prod", "sales.orders")},
        )
        schema_diff(current, pred)          # what will the schema become?

    `dialect` is the SQLGlot dialect used to parse the query and read the upstream types
    (default "duckdb", matching lake-sift's engine).
    """

    def __init__(
        self,
        sql: str,
        upstreams: Mapping[str, Upstream],
        *,
        dialect: str = "duckdb",
    ):
        self.sql = sql
        self.upstreams = dict(upstreams)
        self.dialect = dialect

    def _upstream_mapping(self, con: "duckdb.DuckDBPyConnection") -> dict[str, dict[str, str]]:
        """{table -> {column -> duckdb type string}} for SQLGlot's schema resolver."""
        mapping: dict[str, dict[str, str]] = {}
        for name, up in self.upstreams.items():
            if isinstance(up, pa.Schema):
                mapping[name] = _schema_of(con.from_arrow(up.empty_table()))
            else:
                mapping[name] = _probe_schema(con, up)
        return mapping

    def arrow_schema(self) -> pa.Schema:
        """Predicted output schema as a pyarrow.Schema (no data read)."""
        require("sqlglot", "sql")
        from sqlglot import exp, parse_one
        from sqlglot.optimizer import optimize

        import duckdb

        con = duckdb.connect()
        try:
            mapping = self._upstream_mapping(con)
            optimized = optimize(
                parse_one(self.sql, dialect=self.dialect), schema=mapping, dialect=self.dialect
            )
            fields: list[pa.Field] = []
            for proj in optimized.selects:
                name = proj.alias_or_name
                dtype = proj.type
                if dtype is None or dtype.is_type(exp.DataType.Type.UNKNOWN):
                    # SQLGlot could not resolve the type (e.g. an unknown function) — keep the
                    # column so its presence is still checked, but with an unknown (null) type.
                    fields.append(pa.field(name, pa.null()))
                    continue
                ddl = dtype.sql(dialect="duckdb")
                # Normalize the predicted type through DuckDB so it matches how real sources'
                # types are reported (both go through the same engine).
                arrow_t = con.from_query(f"SELECT CAST(NULL AS {ddl}) AS x").arrow().schema.field(0).type
                fields.append(pa.field(name, arrow_t))
            return pa.schema(fields)
        finally:
            con.close()

    def to_relation(
        self,
        con: "duckdb.DuckDBPyConnection",
        *,
        columns: Any = None,
    ) -> "duckdb.DuckDBPyRelation":
        raise DiffError(
            "SqlSchemaSource predicts a schema only (no data); use schema_diff(), not diff()."
        )

    def __repr__(self) -> str:  # pragma: no cover
        return f"SqlSchemaSource(upstreams={list(self.upstreams)!r})"
