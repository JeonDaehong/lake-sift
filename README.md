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

Being format-native also buys speed rather than costing it: `--preview` bounds a diff from
Iceberg's own manifests in milliseconds, reading no data — no Spark, and no change
tracking that had to be enabled before the change you want to inspect.

## Features

- **Schema diff** — added / removed columns, type changes.
- **Row diff** — keys present only on one side (added / removed).
- **Cell diff** — for shared keys, per-column `old → new` changes.
- **Single & composite keys**, with duplicate-key detection.
- **`NULL == NULL` treated as equal** (unlike default SQL semantics).
- **Column scoping** — `--columns` (only these) / `--exclude` (skip these, e.g. `updated_at`).
- **Schema-only mode** — `--schema-only` compares just the schemas (no key, no data read) as a pre-execution / contract gate.
- **Predicted-schema diff** — `SqlSchemaSource` infers a SQL query's output schema (via SQLGlot) to gate a change *before it runs*.
- **Diff preview** — `--preview` bounds a diff from Iceberg metadata alone (no data read): what it would cost, which partitions can differ, and a *proof* that a change touched no existing row.
- **Output modes** — human-readable color, machine-readable JSON, a Markdown report (for PR comments / CI step summaries), or summary-only.
- **CI-friendly exit codes** — `0` equal, `1` differences, `2` error.
- **Single-node engine** — heavy comparison runs as DuckDB SQL; Python is a thin orchestrator.

## Common workflows

The real situations teams reach for `lake-sift`. Each is a short recipe — the
[Usage](#usage) section below has the full flag reference and the Python API.

### 1. Review a data-pipeline change the way you review code

You changed a SQL model in a PR. You want to see *how the output moves* — not
whether the source data happened to drift overnight. Pin **one immutable input**,
run the old and the new model against it, and diff the two outputs, so every
difference is attributable to your change and nothing else:

```bash
python examples/deployment-gate/run_gate.py \
  --input orders=./_pinned/orders.parquet \
  --old model_old.sql --new model_new.sql --key order_id
# +12 added  -0 removed  ~1043 changed rows (1043 cells)   ← caused by your model change alone
```

Try it with zero setup: `python examples/deployment-gate/run_gate.py --demo`.
Full write-up in [`examples/deployment-gate/`](examples/deployment-gate).

### 2. Block a breaking schema change *before* the pipeline runs

A PR renames or drops a column a downstream table depends on. Catch it
statically — no data read, no pipeline run — by diffing the schema the new query
*would* produce against the live table:

```bash
lake-sift "iceberg:prod/sales.orders" "sql:model.sql" --schema-only \
  -u orders="iceberg:prod/sales.orders"
# - column discount (DOUBLE)     ← the downstream contract would break; exit 1 fails the check
```

### 3. Know a diff's blast radius *before* you run it

Diffing a billion-row table is expensive, and most of the time the answer is "almost
nothing changed". Ask the table's own metadata first — `--preview` reads manifests only,
never data, and answers in milliseconds:

```bash
lake-sift "iceberg:prod/sales.orders@1001" "iceberg:prod/sales.orders@1042" -k order_id --preview
# blast radius (from metadata only, no data read)
#   files            2 of 822 differ  410 shared → provably identical
#   rows to scan     12,480 of 2,400,012,480  0.0% of a full diff
#   bytes to scan    8.4 MB of 91.2 GB
#   partitions       1 touched  dt='2026-07-15'
# proof (from key ranges)
#   provably added   12,480 rows
#   provably removed 0 rows
#   may have changed at most 0 rows  pure append/delete: no existing row is touched
```

The last line is a *proof*, not an estimate: nothing existing was modified. Details in
[Metadata-only diff preview](#metadata-only-diff-preview).

### 4. See exactly what a job just did (`@^`)

A Spark job finished and committed to Iceberg. What did it actually change? The table
only records where it is *now* — but every commit has a parent, so `@^` makes "just
before this ran" addressable. Drop this in as the job's final task; concurrent writers
don't pollute it (their rows land in later snapshots, outside the range):

```bash
# current snapshot vs. its parent = exactly this commit's added / removed / changed rows
lake-sift "iceberg:prod/sales.orders@^" "iceberg:prod/sales.orders" -k order_id
```

Iceberg's own `snapshot.summary` counts *files*, so a one-cell fix reads as
"deleted 2, added 2"; lake-sift reports the actual added/removed/**changed cells**.
(`@^` isolates one commit cleanly when that commit is a single snapshot — see the
caveat under [Iceberg snapshots & branches](#iceberg-snapshots--branches).)

### 5. Audit a change before you publish it (Write-Audit-Publish)

Write to an isolated staging branch, diff it against `main`, and only merge if the
diff is what you expect. The non-zero exit code makes it an orchestration gate:

```bash
lake-sift "iceberg:prod/sales.orders@main" "iceberg:prod/sales.orders@staging" -k order_id \
  || echo "staging differs from main — review before publishing"
```

### 6. Validate a migration, backfill, or export

Did the Parquet export match the live table? Did a backfill land exactly the rows
you expected? Mix formats freely — the diff reads the same across all of them:

```bash
lake-sift export.parquet "iceberg:prod/sales.orders@1042" -k order_id   # export vs pinned snapshot
lake-sift "delta:/data/sales@11" "delta:/data/sales@12" -k order_id     # audit two Delta versions
```

### 7. Turn any of the above into a CI gate with a PR comment

Drop the diff into GitHub Actions: it writes the report to the job summary, posts
it as a sticky PR comment, and fails the check when the data differs. Copy a
workflow from [`examples/github-actions/`](examples/github-actions):

```yaml
- uses: JeonDaehong/lake-sift@v0.5.0
  with:
    left: "iceberg:prod/sales.orders@main"
    right: "iceberg:prod/sales.orders@staging"
    key: order_id
    extras: iceberg
    fail-on-diff: "true"   # block the PR when the data differs
    comment: "true"        # post the diff as a sticky PR comment
```

> **Tip — pin immutable refs.** A diff is only meaningful when both sides are fixed
> points in time. Prefer files, Iceberg **snapshot ids** (`@1042`), or Delta
> **versions** (`@12`); a moving branch like `@main` advances with concurrent
> writes and shows those in-between rows as spurious diffs. See
> [Iceberg snapshots & branches](#iceberg-snapshots--branches) for details.

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
`--summary`, `--schema-only`, `--structural-only`, `--preview`, `--upstream/-u`,
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

### Metadata-only diff preview

`--preview` answers *"is it worth running the real diff, and what would it cost?"* — from
the table's metadata alone, reading **no data at all**. It is the value-level counterpart
to `--schema-only`: that gate moves a *schema* check before the pipeline runs, this one
bounds a *value* diff before it reads a byte.

It works because of one fact about the lakehouse: **data files are immutable, and
snapshots share them.** A file present on both sides is provably yielding identical rows
on both sides, so it can be excluded without opening it:

> real diff ⊆ rows in (left-only files ∪ right-only files)

That is a **sound upper bound** — never a false negative. Per-column bounds in the
manifests tighten it further: a left-only file whose **key range** overlaps no right-only
file cannot share a key with the other side, so its rows are pure removals — no cell of
theirs can have "changed". When nothing overlaps at all, the change is provably a pure
append/delete: not one existing row was touched.

```bash
# What could this snapshot change? (a few manifest reads; no data files opened)
lake-sift "iceberg:prod/sales.orders@1001" "iceberg:prod/sales.orders@1042" -k order_id --preview

# Audit a WAP staging branch before merging — instantly
lake-sift "iceberg:prod/sales.orders@main" "iceberg:prod/sales.orders@staging" -k order_id --preview

# JSON / Markdown for a CI step summary or PR comment
lake-sift "iceberg:prod/sales.orders@1001" "iceberg:prod/sales.orders@1042" -k order_id --preview --json
```

`--key` is optional: without it you still get the cost (files/rows/bytes/partitions), and
with it you also get the key-range proofs. Exit codes follow the usual convention, with a
sharper meaning: **`0` = provably identical** (no file differs — certain, with no data
read), `1` = the sides *may* differ, `2` = error.

**What it proves, and what it doesn't.** These are the honest limits:

| Claim | Status |
|---|---|
| No file differs → the sides are **identical** | Proven |
| Key ranges don't overlap → **no existing row was modified** | Proven |
| `provably added` / `provably removed` row counts | Lower bounds on the real diff |
| `may have changed: at most N` | Upper bound on the real diff |
| Files differ → the *values* differ | **Not** implied — a compaction rewrites files without changing a value, so the bound is loose but never wrong |
| A column is **unchanged** | **Not** provable — bounds are aggregates, and values can permute inside an unchanged range |

Preview never claims more than it can prove, so a `0` is safe to gate on and a non-zero
means "read the data to know". Requires the `iceberg` extra; both sides must be Iceberg
sources (any other source exits `2` rather than silently guessing). It is most useful on
two snapshots/branches of the *same* table, where file sharing is high — on unrelated
tables it stays correct and simply reports that a full scan is needed.

Two details follow from "a shared file must yield identical rows on both sides":

- **Merge-on-read deletes are handled.** A delete file is part of a data file's identity,
  so a shared data file carrying a different delete set is correctly treated as differing.
  The same goes for the column projection: the same bytes read through a different
  `selected_fields` are not the same rows, and are not reported as shared.
- **A `row_filter` cannot be previewed** (exit `2`). Manifests count *whole files*, and
  planning only prunes files that cannot match — nothing says how many rows inside a
  surviving file pass the filter. Every count would be an overcount, so preview refuses
  rather than report a number that isn't true. Preview the unfiltered source, or run the
  full diff (which filters normally).

From Python, `preview(left, right, key=[...])` returns a `PreviewResult`; it reads no data
and owns no connection, so it needs no closing:

```python
from lakesift import preview, IcebergSource

p = preview(
    IcebergSource.from_catalog("prod", "sales.orders", ref="main"),
    IcebergSource.from_catalog("prod", "sales.orders", ref="staging"),
    key=["order_id"],
)
p.is_empty()          # True -> provably identical, and nothing was read
p.is_pure_append()    # True -> no existing row can have been modified
p.rows_to_scan        # what a real diff would still have to read
p.scan_fraction       # ... as a share of a full diff (0.0 - 1.0)
p.max_changed_rows    # upper bound on rows whose cells may have changed
p.partitions_touched  # [{"dt": "2026-07-15"}, ...]
p.to_dict()           # / p.to_json()
```

### Iceberg snapshots & branches

Either operand may be an Iceberg table instead of a file, using the form
`iceberg:<catalog>/<namespace>.<table>[@<snapshot_id-or-ref>[^]]`. After `@`, an
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

**Isolate a single commit with `@^`.** Append `^` to an Iceberg operand to reference
that snapshot's **parent**. This makes "the table just before my job ran" addressable
from a table that only records where it is now — diff the current snapshot against its
parent to see exactly what the last commit did, no matter what else ran concurrently
(concurrent writes land in later snapshots, outside the range):

```bash
# What did the most recent commit change? (current snapshot vs. its parent)
lake-sift "iceberg:prod/sales.orders@^" "iceberg:prod/sales.orders" -k order_id

# The final task of a Spark job that just wrote the table — self-contained, no
# "before" state to capture up front:
#   lake-sift "iceberg:prod/sales.orders@^" "iceberg:prod/sales.orders" -k order_id --markdown
```

`@^` also composes with an explicit snapshot (`@1042^`) or a branch head (`@main^`).

> **Caveat — one logical change, one snapshot.** `@^` steps back exactly one snapshot,
> so it isolates a commit cleanly only when that commit *is* one snapshot. Spark's
> `INSERT INTO`/`MERGE` commit atomically and fit. Some writers split a change across
> two snapshots (pyiceberg's `overwrite` emits a DELETE then an APPEND), where `@^`
> would step back only half of it. When in doubt, stamp the before/after snapshot ids
> yourself around the job and diff those.

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
Write-Audit-Publish), `parent=True` (the resolved snapshot's parent, i.e. `@^`), row
filter, and field projection pushed down to the scan:

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

# Or isolate what the latest commit did: current snapshot vs. its parent
before = IcebergSource.from_catalog("prod", "sales.orders", parent=True)
after = IcebergSource.from_catalog("prod", "sales.orders")
with diff(before, after, key=["order_id"]) as result:
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
│   ├── preview.py       # metadata-only blast-radius preview (reads no data)
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
