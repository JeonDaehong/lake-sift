# lake-sift

[![PyPI](https://img.shields.io/pypi/v/lake-sift.svg)](https://pypi.org/project/lake-sift/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![CI](https://github.com/JeonDaehong/lake-sift/actions/workflows/ci.yml/badge.svg)](https://github.com/JeonDaehong/lake-sift/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)

**Value-level data diff for the lakehouse era.**

`lake-sift` compares two datasets down to the individual cell — on a single node,
with **no Spark, no warehouse, and no framework lock-in**. It diffs Parquet files
Iceberg snapshots, and Delta versions today, mixing them freely through
pluggable source adapters (see the [roadmap](#roadmap)).

```console
$ lake-sift a.parquet b.parquet --key id
+1 added  -1 removed  ~1 changed row (1 cell)
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
- **Three output modes** — human-readable color, machine-readable JSON, summary-only.
- **CI-friendly exit codes** — `0` equal, `1` differences, `2` error.
- **Single-node engine** — heavy comparison runs as DuckDB SQL; Python is a thin orchestrator.

## Installation

```bash
pip install lake-sift             # once published to PyPI
pip install "lake-sift[iceberg]"  # with the Iceberg source (PyIceberg)
pip install "lake-sift[delta]"    # with the Delta source (delta-rs)
```

Until the first PyPI release, install from source:

```bash
git clone https://github.com/JeonDaehong/lake-sift.git
cd lake-sift
pip install -e ".[dev]"
```

Requires Python 3.10+. The Iceberg and Delta sources are optional extras —
Parquet diffing needs no extra dependencies.

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

Flags: `--key/-k`, `--exclude/-x`, `--columns/-c`, `--json`, `--summary`,
`--allow-duplicates`, `--tolerance/-t`, `--ignore-case/-i`, `--sample/-n`, `--top`.

**Column projection (pushdown).** When you narrow the comparison with `--columns`
or `--exclude`, lake-sift reads only the key plus the compared columns from each
source — pushed down to the scan, so Iceberg/Delta/Parquet never materialize
columns you don't compare. A consequence: added/removed rows then show only those
columns. Schema changes are still detected across the *full* schema (read from
metadata), so a dropped or retyped column is reported even when it isn't compared.
Without these flags, the full rows are read and shown as before.

### Iceberg snapshots

Either operand may be an Iceberg table instead of a file, using the form
`iceberg:<catalog>/<namespace>.<table>[@<snapshot_id>]`. Catalog connection
details are read from PyIceberg's standard config
([`~/.pyiceberg.yaml`](https://py.iceberg.apache.org/configuration/) or
`PYICEBERG_*` environment variables) — lake-sift only references a catalog by name.

```bash
# Diff two snapshots of the same Iceberg table (audit a change)
lake-sift "iceberg:prod/sales.orders@1001" "iceberg:prod/sales.orders@1042" -k order_id

# Mix sources freely: validate a Parquet export against the live table
lake-sift export.parquet "iceberg:prod/sales.orders" -k order_id
```

Requires the `iceberg` extra (`pip install "lake-sift[iceberg]"`). For finer
control (row filters, field projection, an already-loaded table) use
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

### Python API

The CLI is a thin wrapper over the library — both share the same core.

```python
from lakesift import diff, ParquetSource

result = diff(
    left=ParquetSource("a.parquet"),
    right=ParquetSource("b.parquet"),
    key=["id"],
    exclude=["updated_at"],
)

result.is_empty()      # True when there is no difference (the common CI check)
result.summary()       # {"added": 1, "removed": 1, "changed": 1, ...}
result.schema_changes  # [SchemaChange(...), ...]
result.added           # rows only on the right
result.removed         # rows only on the left
result.changed_cells   # [CellChange(key=..., column=..., old=..., new=...), ...]
result.to_json()
```

`IcebergSource` reads a snapshot through PyIceberg and accepts a loaded table
directly, or loads one from a catalog — with optional snapshot pinning, row
filter, and field projection pushed down to the scan:

```python
from lakesift import diff, IcebergSource

left = IcebergSource.from_catalog("prod", "sales.orders", snapshot_id=1001)
right = IcebergSource.from_catalog(
    "prod", "sales.orders", snapshot_id=1042,
    row_filter="region = 'EU'",          # narrow the scan before diffing
)

with diff(left, right, key=["order_id"]) as result:
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

## Roadmap

| Version | Scope |
|---|---|
| v0.1 | Parquet MVP — schema/row/cell diff, CLI, exit codes |
| v0.2 | numeric tolerance, ignore-case, `--sample`, top-k changed columns |
| v0.3 | Iceberg snapshot source (PyIceberg) — same core, new adapter |
| **v0.4** | **Delta version source** (delta-rs) — same core, new adapter *(current)* |
| v0.5 | HTML report, GitHub Action |
| v1.0 | stable API + documentation site |

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
│   ├── sources/         # input adapters (parquet, iceberg, delta)
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

Please keep changes within the documented v0 scope unless a roadmap item is
being implemented.

## License

[MIT](LICENSE) © JeonDaehong
