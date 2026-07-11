# Deployment gate — attributing a data diff to a *code* change

A value-level diff only means something when the two sides differ for **one**
reason. The naive "post-deploy" check breaks that rule:

> compare *yesterday's production table* against *today's freshly built table*

That diff mixes **two** causes and cannot separate them:

1. the pipeline code you changed — *what you want to review*, and
2. source rows that arrived or changed in between — *noise*.

So the diff is **confounded by time** and is meaningless as a gate for a code
change. (This is exactly why lake-sift's docs insist on pinning an *immutable*
ref — a snapshot id or version — rather than a moving `@main`.)

## The fix: hold the input constant

Remove the time variable. Pin **one immutable input snapshot**, run the **old**
code and the **new** code against *that same input*, and diff the two outputs.
Both runs read identical bytes, so every difference is caused by the code change —
nothing else.

```
pinned input  ─┬─▶  OLD sql  ─▶  output_old ─┐
(immutable)    │                             ├─▶  lake-sift diff  ─▶  gate (exit 0/1)
               └─▶  NEW sql  ─▶  output_new ─┘
```

lake-sift is only the **comparator** on the right. This example adds the thin
orchestration on the left — pin, run old, run new — that makes the comparison
fair. It stays single-node and framework-free (one DuckDB engine, the same one
lake-sift diffs with); it is the single-node analogue of Datafold / SQLMesh
`table_diff`.

## Try it (zero setup)

```bash
pip install lake-sift
python run_gate.py --demo
```

The demo writes a pinned `orders` table and two model versions, runs the gate,
and prints the attributable diff — added rows (refunds now included) and changed
cells (amount rounded), *all* caused by the model change because the input was
identical for both runs.

## Use it on a real change

```bash
python run_gate.py \
    --input orders=./_pinned/orders.parquet \
    --old model_old.sql \
    --new model_new.sql \
    --key order_id
```

- `--input NAME=PATH` — a pinned input table (repeatable). `NAME` is the table
  name referenced in the SQL; `PATH` is an immutable Parquet file. In a
  lakehouse, export/pin an Iceberg snapshot id or Delta version to a file first —
  the point is only that **both runs read the same bytes**.
- `--old` / `--new` — the SQL model before and after your change.
- `--key` — the row identity key. `--exclude` and `--tolerance` are passed
  through to lake-sift.

Exit codes match the CLI: `0` identical · `1` differences · `2` error — so it
drops straight into CI. See [`deployment-gate.yml`](deployment-gate.yml) for a
GitHub Actions workflow that fetches the old model from the base branch and gates
the PR.

## When to reach for which gate

| Gate | What it catches | Reads data? |
|---|---|---|
| `--schema-only` + `sql:` prediction | schema/contract break (dropped, renamed, retyped column) | no — fully static, before it runs |
| **this deployment gate** | **value/row changes caused by the code change** | yes — old vs new output on a pinned input |
| `wap-gate.yml` (Write-Audit-Publish) | a staging branch drifting from `main` before publish | yes — live tables under your control |

The schema gate is the cheapest and runs earliest; this value gate is what you
add when you need to see *how the numbers move*, not just whether a column
survived.
