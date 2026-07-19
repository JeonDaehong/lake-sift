"""Execute a run and record the outcome — the bridge from the UI to the library API.

The web layer never re-implements comparison logic; it calls the same `diff` /
`schema_diff` / `preview` the CLI does, reusing the CLI's operand parser (`_source`) so
that `iceberg:`, `delta:`, `sql:` and Parquet paths mean exactly what they mean on the
command line. Results are streamed, so we pull only a bounded *sample* of the rows/cells
into the history store — the badge and counts are exact, the row listing is capped.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from itertools import islice
from typing import Any, Iterable, Optional

from lakesift.cli import _parse_upstreams, _source
from lakesift.core import diff, schema_diff
from lakesift.preview import preview as preview_diff

# How many rows/cells to keep per run in the history store. The counts shown are always
# exact (from the streaming aggregates); only the row *listing* is truncated to this.
_ROW_CAP = 200
_CELL_CAP = 500

_PREFIXES = (("iceberg:", "iceberg"), ("delta:", "delta"), ("sql:", "sql"))


def source_kind(spec: str) -> str:
    for prefix, name in _PREFIXES:
        if spec.startswith(prefix):
            return name
    return "parquet"


def pair_kind(left: str, right: str) -> str:
    lk, rk = source_kind(left), source_kind(right)
    return lk if lk == rk else "mixed"


@dataclass
class RunRequest:
    """Everything needed to run one comparison, and to re-run it later."""

    mode: str  # 'diff' | 'schema' | 'preview'
    left: str
    right: str
    key: list[str] = field(default_factory=list)
    exclude: list[str] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)
    tolerance: Optional[float] = None
    ignore_case: bool = False
    allow_duplicates: bool = False
    structural_only: bool = False
    upstreams: list[str] = field(default_factory=list)  # ["name=source", ...]
    sql_dialect: str = "duckdb"

    def to_params(self) -> dict[str, Any]:
        return dict(self.__dict__)

    @classmethod
    def from_params(cls, params: dict[str, Any]) -> "RunRequest":
        known = {k: params[k] for k in cls.__dataclass_fields__ if k in params}
        return cls(**known)


def _cap(items: Iterable[Any], limit: int) -> list[Any]:
    return list(islice(items, limit))


def _schema_change_dicts(result: Any) -> list[dict[str, Any]]:
    return [
        {"column": c.column, "kind": c.kind, "old_type": c.old_type, "new_type": c.new_type}
        for c in result.schema_changes
    ]


def _diff_payload(result: Any) -> tuple[str, dict, dict]:
    summary = result.summary()
    detail = {
        "schema_changes": _schema_change_dicts(result),
        "changed_by_column": [
            {"column": c, "count": n} for c, n in result.changed_by_column
        ],
        "added": _cap(result.added, _ROW_CAP),
        "removed": _cap(result.removed, _ROW_CAP),
        "changed_cells": [
            {"key": c.key, "column": c.column, "old": c.old, "new": c.new}
            for c in _cap(result.changed_cells, _CELL_CAP)
        ],
        "caps": {"rows": _ROW_CAP, "cells": _CELL_CAP},
    }
    status = "identical" if result.is_empty() else "differences"
    return status, summary, detail


def _schema_payload(result: Any) -> tuple[str, dict, dict]:
    changes = _schema_change_dicts(result)
    detail = {"schema_changes": changes}
    summary = {"schema_changes": len(changes)}
    status = "identical" if not changes else "schema"
    return status, summary, detail


def _preview_payload(result: Any) -> tuple[str, dict, dict]:
    d = result.to_dict()
    summary = {
        "files_differing": d["files"]["differing"],
        "rows_to_scan": d["scan"]["rows_to_scan"],
        "rows_total": d["scan"]["rows_total"],
        "schema_changes": len(d["schema_changes"]),
    }
    status = "identical" if result.is_empty() else "differences"
    return status, summary, d


def _resolve(spec: str, req: RunRequest):
    upstreams = _parse_upstreams(req.upstreams or None)
    return _source(spec, upstreams=upstreams, dialect=req.sql_dialect)


def execute(store, run_id: int, req: RunRequest) -> None:
    """Run the comparison and write the result (or the error) back to the store.

    Any exception is caught and recorded as an 'error' run — a bad operand or an
    unreadable table must not take down the worker thread or the server.
    """
    start = time.monotonic()
    try:
        if req.mode == "preview":
            result = preview_diff(
                _resolve(req.left, req), _resolve(req.right, req), key=req.key or None
            )
            status, summary, detail = _preview_payload(result)
        elif req.mode == "schema":
            result = schema_diff(
                _resolve(req.left, req),
                _resolve(req.right, req),
                compare_types=not req.structural_only,
            )
            status, summary, detail = _schema_payload(result)
        else:
            with diff(
                left=_resolve(req.left, req),
                right=_resolve(req.right, req),
                key=req.key,
                exclude=req.exclude or None,
                columns=req.columns or None,
                allow_duplicates=req.allow_duplicates,
                tolerance=req.tolerance,
                ignore_case=req.ignore_case,
            ) as result:
                status, summary, detail = _diff_payload(result)
        ms = int((time.monotonic() - start) * 1000)
        store.finish(run_id, status=status, summary=summary, detail=detail, duration_ms=ms)
    except Exception as e:  # noqa: BLE001 - any failure becomes an 'error' run
        ms = int((time.monotonic() - start) * 1000)
        store.finish(run_id, status="error", error=str(e), duration_ms=ms)
