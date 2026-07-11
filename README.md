# lake-sift

[![PyPI](https://img.shields.io/pypi/v/lake-sift.svg)](https://pypi.org/project/lake-sift/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![CI](https://github.com/JeonDaehong/lake-sift/actions/workflows/ci.yml/badge.svg)](https://github.com/JeonDaehong/lake-sift/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)

**Value-level data diff for the lakehouse era.**

`lake-sift` compares two datasets down to the individual cell — on a single node,
with **no Spark, no warehouse, and no framework lock-in**. It diffs Parquet files,
Iceberg snapshots, and Delta versions today, mixing them freely through pluggable
source adapters.

```console
$ lake-sift a.parquet b.parquet --key id
+1 added  -1 removed  ~1 changed rows (1 cells)
  top changed columns: v (1)
- id=1, v='a'
+ id=4, v='d'
~ [id=3] v: 'c' → 'C'
```

It is a library with a thin CLI on top, so the same diff powers both an
interactive review and a CI gate (via exit codes).

## Why lake-sift

Most data-diff tools are bound to something heavier — a warehouse, a transform
framework, a JVM cluster, or a catalog. `lake-sift` deliberately stays small and
unbound:

| Existing tool | Bound to |
|---|---|
| Datafold / Recce | a warehouse + dbt workflow |
| SQLMesh `table_diff` | the SQLMesh framework |
| lakeFS `refs_data_diff` | lakeFS + Spark + a JAR |
| Iceberg changelog / Delta CDF | Spark/JVM, change tracking enabled up front |
| reladiff | DB connections (not files/snapshots) |

**lake-sift's niche:** engine-neutral · single-node · framework-free ·
format-native · review-oriented output.

## Features

- **Schema diff** — added / removed columns, type changes.
- **Row diff** — keys present only on one side (added / removed).
- **Cell diff** — for shared keys, per-column `old → new` changes.
- **Single & composite keys**, with duplicate-key detection.
- **`NULL == NULL` treated as equal** (unlike default SQL semantics).
- **Column scoping** — `--columns` (only these) / `--exclude` (skip these, e.g. `updated_at`).
- **Schema-only mode** — `--schema-only` compares just the schemas (no key, no data read) as a pre-execution / contract gate.
- **Predicted-schema diff** — `SqlSchemaSource` infers a SQL query's output schema (via SQLGlot) to gate a change *before it runs*.
- **Output modes** — human-readable color, machine-readable JSON, a Markdown report (for PR comments / CI step summaries), or summary-only.
- **CI-friendly exit codes** — `0` equal, `1` differences, `2` error.
- **Single-node engine** — heavy comparison runs as DuckDB SQL; Python is a thin orchestrator.

## Installation

```bash
pip install lake-sift             # Parquet diffing (no extra deps)
pip install "lake-sift[iceberg]"  # with the Iceberg source (PyIceberg)
pip install "lake-sift[delta]"    # with the Delta source (delta-rs)
pip install "lake-sift[sql]"      # with SQL output-schema prediction (SQLGlot)
```

Or install from source (for development):

```bash
git clone https://github.com/JeonDaehong/lake-sift.git
cd lake-sift
pip install -e ".[dev]"
```

Requires Python 3.10+. The Iceberg, Delta, and SQL-prediction sources are optional
extras — Parquet diffing needs no extra dependencies.

## Usage

### Command line

```bash
# Compare two files by key
lake-sift a.parquet b.parquet --key id

# Composite key, exclude a volatile column, machine-readable output
lake-sift a.parquet b.parquet -k order_id,line_no -x updated_at --json

# As a CI gate: non-zero exit blocks the change when data differs
lake-sift prod.parquet pr.parquet -k id || echo "data change detected!"
```

Flags: `--key/-k`, `--exclude/-x`, `--columns/-c`, `--json`, `--markdown`,
`--summary`, `--schema-only`, `--structural-only`, `--upstream/-u`,
`--sql-dialect`, `--allow-duplicates`, `--tolerance/-t`, `--ignore-case/-i`,
`--sample/-n`, `--top`, `--version`.

### Schema-only checks (pre-execution gate)

`--schema-only` compares just the two **schemas** — no `--key`, and **no data is
read** (Iceberg/Delta report their schema from metadata; Parquet reads only the
footer). It reports added / removed / retyped columns and nothing else, so it is a
fast structural gate you can run *before* materializing rows — e.g. to catch a
dropped or retyped column that would break a downstream contract.

```bash
# Did the freshly built table drift from the live one's schema? (data is never read)
lake-sift "iceberg:prod/sales.orders@main" "delta:/build/sales.orders" --schema-only
# ~ column amount: INTEGER → DOUBLE
# - column discount (DOUBLE)
```

Exit codes are the usual `0` / `1` / `2`, so it drops into CI the same way. From
Python, use `schema_diff(left, right)` — it returns a `DiffResult` carrying only
`schema_changes` and owns no live connection.

#### Predicting a query's output schema before it runs

The schema check can move even further left — *before the pipeline runs at all*.
`SqlSchemaSource` uses [SQLGlot](https://github.com/tobymao/sqlglot) to infer the
**output schema of a SQL query** from the schemas of its upstream tables, without
executing it or reading a row. Pair the prediction with the live table's schema to
catch a breaking change in code review, the way a reasoner checks an ontology before
it is used:

```python
from lakesift import schema_diff, SqlSchemaSource, IcebergSource

pred = SqlSchemaSource(
    "SELECT id, CAST(amount AS DOUBLE) AS amount FROM orders WHERE status = 'paid'",
    upstreams={"orders": IcebergSource.from_catalog("prod", "sales.orders")},
)
# current live schema (left) vs the schema this query *would* produce (right)
with schema_diff(IcebergSource.from_catalog("prod", "sales.orders"), pred) as r:
    for c in r.schema_changes:
        print(c.kind, c.column)          # e.g. removed discount / type_changed amount
```

The same check is available from the CLI: pass the query as a `sql:<file.sql>`
operand and name each input table with `--upstream/-u NAME=SOURCE` (the source is an
ordinary operand — a Parquet path, `iceberg:…`, or `delta:…`). It only applies under
`--schema-only`:

```bash
# current live schema vs the schema model.sql *would* produce — no data read
lake-sift "iceberg:prod/sales.orders" "sql:model.sql" --schema-only \
  -u orders="iceberg:prod/sales.orders" -u customers=customers.parquet
# ~ column amount: INTEGER → DOUBLE
# - column discount (DOUBLE)

# structure only (ignore best-effort type predictions); --sql-dialect for non-DuckDB SQL
lake-sift "iceberg:prod/sales.orders" "sql:model.sql" --schema-only --structural-only \
  --sql-dialect snowflake -u orders="iceberg:prod/sales.orders"
```

Upstream schemas are read from ordinary sources (metadata only), so the whole
prediction touches no data and needs no warehouse. **Structural** prediction (which
columns are added / dropped / renamed) is reliable; **types** are best-effort — for a
purely structural gate, use `--structural-only` (CLI) or `schema_diff(...,
compare_types=False)` (Python). Requires the `sql` extra (`pip install
"lake-sift[sql]"`).

**Column projection (pushdown).** When you narrow the comparison with `--columns`
or `--exclude`, lake-sift reads only the key plus the compared columns from each
source — pushed down to the scan, so Iceberg/Delta/Parquet never materialize
columns you don't compare. A consequence: added/removed rows then show only those
columns. Schema changes are still detected across the *full* schema (read from
metadata), so a dropped or retyped column is reported even when it isn't compared.
Without these flags, the full rows are read and shown as before. A column named in
`--columns` that exists on *neither* side is treated as an error (exit `2`) rather
than silently ignored, so a typo can't quietly turn a CI gate into a no-op.

### Iceberg snapshots & branches

Either operand may be an Iceberg table instead of a file, using the form
`iceberg:<catalog>/<namespace>.<table>[@<snapshot_id-or-ref>]`. After `@`, an
integer is a snapshot id and anything else is a **branch or tag name**. Catalog
connection details are read from PyIceberg's standard config
([`~/.pyiceberg.yaml`](https://py.iceberg.apache.org/configuration/) or
`PYICEBERG_*` environment variables) — lake-sift only references a catalog by name.

> **Reproducible diffs — pin an immutable ref.** A diff is only meaningful when
> both sides are fixed points in time. A **snapshot id** (`@1042`) is immutable, so
> the same command always yields the same result. A **branch/tag** (`@main`) is a
> *moving* pointer — concurrent writes advance it, so a table diffed against a live
> branch shows rows written in between as spurious added/removed. Diff **files**
> (immutable by nature), **snapshot ids**, or **Delta versions** for a stable gate;
> reserve moving refs for the WAP pattern below, where the staging branch is
> *isolated* from `main` and both sides only move under your control. For a CI gate,
> capture the snapshot id at read time and pin it, rather than re-reading `@main`.

```bash
# Diff two snapshots of the same Iceberg table (audit a change)
lake-sift "iceberg:prod/sales.orders@1001" "iceberg:prod/sales.orders@1042" -k order_id

# Mix sources freely: validate a Parquet export against the live table
lake-sift export.parquet "iceberg:prod/sales.orders" -k order_id
```

**Write-Audit-Publish (WAP).** Write your changes to a staging branch, audit them by
diffing against `main`, and only publish (merge) if the diff is what you expect. The
non-zero exit code makes this a CI/orchestration gate:

```bash
# Audit the staging branch before merging it into main
lake-sift "iceberg:prod/sales.orders@main" "iceberg:prod/sales.orders@staging" -k order_id \
  || echo "staging differs from main — review before publishing"
```

Requires the `iceberg` extra (`pip install "lake-sift[iceberg]"`). For finer
control (branch/tag `ref`, row filters, field projection, an already-loaded table) use
`IcebergSource` from the Python API.

### Delta tables

Either operand may be a Delta Lake table, using the form
`delta:<path-or-uri>[@<version>]`. The path is a local directory or any URI
delta-rs understands (`s3://`, `abfs://`, …); `@<version>` pins a table version
for time travel.

```bash
# Diff two versions of the same Delta table (audit a change)
lake-sift "delta:/data/sales@11" "delta:/data/sales@12" -k order_id

# Mix sources freely: validate a Parquet export against a cloud Delta table
lake-sift export.parquet "delta:s3://lake/sales" -k order_id
```

Requires the `delta` extra (`pip install "lake-sift[delta]"`). For finer control
(column projection, predicate filters, storage credentials, an already-loaded
table) use `DeltaSource` from the Python API.

### Deployment gate — attributing a value diff to a code change

A value diff of *yesterday's production table* vs *today's freshly built table* is
**confounded by time**: it mixes the pipeline change you want to review with source
rows that arrived in between, and you cannot separate them — so it is meaningless as
a gate for a *code* change. The fix is to remove the time variable: pin **one
immutable input snapshot**, run the **old** code and the **new** code against that
same input, and diff the two outputs. Both runs read identical bytes, so every
difference is caused by the code change.

```
pinned input  ─┬─▶  OLD sql  ─▶  output_old ─┐
(immutable)    │                             ├─▶  lake-sift diff  ─▶  gate (exit 0/1)
               └─▶  NEW sql  ─▶  output_new ─┘
```

lake-sift is the comparator on the right; the thin "pin, run old, run new"
orchestration on the left is a copy-pasteable recipe in
[`examples/deployment-gate/`](examples/deployment-gate) (single-node DuckDB, no
warehouse — the framework-free analogue of Datafold / SQLMesh `table_diff`). Try it
with zero setup:

```bash
python examples/deployment-gate/run_gate.py --demo
```

This complements the two static gates above: `--schema-only` (+ `sql:` prediction)
catches a *schema* break before the pipeline runs; the deployment gate catches how
the *values* move once it does.

### GitHub Action

Run the diff as a CI step: it writes the Markdown report to the job summary,
optionally posts it as a sticky pull-request comment, and (by default) fails the
check when the datasets differ — turning any of the diffs above into a merge gate.

```yaml
- uses: actions/setup-python@v5
  with:
    python-version: "3.12"

- uses: JeonDaehong/lake-sift@v0.5.0
  with:
    left: "iceberg:prod/sales.orders@main"
    right: "iceberg:prod/sales.orders@staging"   # audit the WAP staging branch
    key: order_id
    extras: iceberg          # install the source format(s) you use
    fail-on-diff: "true"     # block the PR when the branches differ
    comment: "true"          # post the report as a sticky PR comment
  env:
    PYICEBERG_CATALOG__PROD__URI: ${{ secrets.ICEBERG_CATALOG_URI }}
```

Key inputs: `left`, `right`, `key` (required); `columns`, `exclude`, `tolerance`,
`ignore-case`, `allow-duplicates`, `sample` (passed through to the CLI); `extras`
(`iceberg` / `delta` / `iceberg,delta`), `version`, `fail-on-diff`, `comment`.
Outputs: `diff` (`true`/`false`), `exit-code`, `report` (path to the Markdown file).
Lakehouse credentials are supplied by your workflow's `env`/`secrets` — the action
only invokes lake-sift. Full examples live in
[`examples/github-actions/`](examples/github-actions).

### Python API

The CLI is a thin wrapper over the library — both share the same core.

The result owns a live DuckDB connection (rows/cells are streamed), so use it as a
context manager. `added`/`removed`/`changed_cells` return a fresh **iterator** on each
access — use `summary()` for counts and `list(...)` for the full list.

```python
from lakesift import diff, ParquetSource

with diff(
    left=ParquetSource("a.parquet"),
    right=ParquetSource("b.parquet"),
    key=["id"],
    exclude=["updated_at"],
) as result:
    result.is_empty()      # True when there is no difference (the common CI check)
    result.summary()       # {"added": 1, "removed": 1, "changed": 1, "changed_cells": 1, "schema_changes": 0}
    result.schema_changes  # [SchemaChange(...), ...]
    result.added           # iterator of rows only on the right
    result.removed         # iterator of rows only on the left
    result.changed_cells   # iterator of CellChange(key=..., column=..., old=..., new=...)
    result.to_json()
```

`IcebergSource` reads through PyIceberg and accepts a loaded table directly, or loads
one from a catalog — with optional snapshot pinning, **branch/tag `ref`** (for
Write-Audit-Publish), row filter, and field projection pushed down to the scan:

```python
from lakesift import diff, IcebergSource

# Audit a staging branch against main before publishing (WAP)
main = IcebergSource.from_catalog("prod", "sales.orders", ref="main")
staging = IcebergSource.from_catalog(
    "prod", "sales.orders", ref="staging",
    row_filter="region = 'EU'",          # narrow the scan before diffing
)

with diff(main, staging, key=["order_id"]) as result:
    print(result.summary())
```

`DeltaSource` reads a table through delta-rs and accepts a path/URI or an
already-loaded `DeltaTable`, with optional version time travel, column
projection, predicate filters, and storage credentials:

```python
from lakesift import diff, DeltaSource

left = DeltaSource("/data/sales", version=11)
right = DeltaSource(
    "/data/sales", version=12,
    columns=["order_id", "amount", "status"],  # project before diffing
)

with diff(left, right, key=["order_id"]) as result:
    print(result.summary())
```

### Exit codes

| Code | Meaning |
|---|---|
| `0` | Identical — no differences |
| `1` | Differences found |
| `2` | Error — comparison not possible (missing key, unreadable input, duplicate keys, …) |

## Non-goals

`lake-sift` does one thing — diff. It is intentionally **not** a catalog or
version-control system (lakeFS, Nessie), a table-maintenance/optimization tool,
a transformation framework (dbt, SQLMesh), or a monitoring/observability
platform.

## Project layout

```
lake-sift/
├── src/lakesift/
│   ├── core.py          # diff engine (DuckDB SQL generation/execution)
│   ├── result.py        # DiffResult, CellChange, SchemaChange
│   ├── sources/         # input adapters (parquet, iceberg, delta, sql schema prediction)
│   ├── render/          # human (color) and json renderers
│   └── cli.py           # typer CLI
└── tests/
```

## Contributing

Issues and pull requests are welcome. To set up a development environment:

```bash
pip install -e ".[dev]"
pytest
```

Please keep changes focused and aligned with the project's scope — `lake-sift`
does one thing: diff.

## License

[MIT](LICENSE) © JeonDaehong
