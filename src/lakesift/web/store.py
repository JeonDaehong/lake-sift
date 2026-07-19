"""SQLite-backed run history — the "build log" behind the web UI.

One row per diff/schema/preview run, like a Jenkins build or an Airflow DAG run: it
records what was compared, the outcome badge, a summary, a bounded sample of the changes,
and how long it took. Kept deliberately small — the store is an index of past runs, not a
copy of the datasets. Big results are sampled (see runner._cap) before they land here.

Each call opens its own short-lived connection so the store is safe to touch from the
request thread and the background worker thread at once (WAL mode, one writer at a time).
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    kind         TEXT    NOT NULL,          -- 'diff' | 'schema' | 'preview'
    status       TEXT    NOT NULL,          -- running|identical|differences|schema|error
    left         TEXT    NOT NULL,
    right        TEXT    NOT NULL,
    key          TEXT,                       -- comma-separated key columns
    source_kind  TEXT    NOT NULL,           -- parquet|iceberg|delta|sql|mixed
    params       TEXT    NOT NULL,           -- json: the full run request (for re-run)
    summary      TEXT,                       -- json: counts
    detail       TEXT,                       -- json: schema changes + sampled rows/cells
    error        TEXT,
    duration_ms  INTEGER,
    created_at   REAL    NOT NULL            -- unix seconds
);
CREATE INDEX IF NOT EXISTS runs_created_idx ON runs (created_at DESC);
"""


def _dumps(obj: Any) -> Optional[str]:
    # default=str keeps datetimes/decimals/bytes from blowing up serialization.
    return None if obj is None else json.dumps(obj, default=str, ensure_ascii=False)


def _loads(text: Optional[str]) -> Any:
    return None if text is None else json.loads(text)


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    d["params"] = _loads(d.get("params"))
    d["summary"] = _loads(d.get("summary"))
    d["detail"] = _loads(d.get("detail"))
    return d


class RunStore:
    def __init__(self, path: str | Path):
        self.path = str(path)
        parent = Path(self.path).parent
        if parent and not parent.exists():
            parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as con:
            con.executescript(_SCHEMA)

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.path, timeout=30.0)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA busy_timeout=30000")
        return con

    def create(
        self,
        *,
        kind: str,
        left: str,
        right: str,
        key: Optional[str],
        source_kind: str,
        params: dict[str, Any],
    ) -> int:
        """Insert a run in the 'running' state and return its id."""
        with self._connect() as con:
            cur = con.execute(
                "INSERT INTO runs (kind, status, left, right, key, source_kind, params, created_at)"
                " VALUES (?, 'running', ?, ?, ?, ?, ?, ?)",
                (kind, left, right, key, source_kind, _dumps(params), time.time()),
            )
            return int(cur.lastrowid)

    def finish(
        self,
        run_id: int,
        *,
        status: str,
        summary: Optional[dict[str, Any]] = None,
        detail: Optional[dict[str, Any]] = None,
        error: Optional[str] = None,
        duration_ms: Optional[int] = None,
    ) -> None:
        with self._connect() as con:
            con.execute(
                "UPDATE runs SET status=?, summary=?, detail=?, error=?, duration_ms=? WHERE id=?",
                (status, _dumps(summary), _dumps(detail), error, duration_ms, run_id),
            )

    def get(self, run_id: int) -> Optional[dict[str, Any]]:
        with self._connect() as con:
            row = con.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        return _row_to_dict(row) if row is not None else None

    def list(self, *, kind: Optional[str] = None, limit: int = 200) -> list[dict[str, Any]]:
        sql = "SELECT * FROM runs"
        args: list[Any] = []
        if kind:
            sql += " WHERE kind=?"
            args.append(kind)
        sql += " ORDER BY id DESC LIMIT ?"
        args.append(limit)
        with self._connect() as con:
            rows = con.execute(sql, args).fetchall()
        return [_row_to_dict(r) for r in rows]

    def counts_by_status(self) -> dict[str, int]:
        with self._connect() as con:
            rows = con.execute(
                "SELECT status, count(*) AS n FROM runs GROUP BY status"
            ).fetchall()
        return {r["status"]: r["n"] for r in rows}

    def delete(self, run_id: int) -> bool:
        """Remove a run from history. Returns True if a row was deleted."""
        with self._connect() as con:
            cur = con.execute("DELETE FROM runs WHERE id=?", (run_id,))
            return cur.rowcount > 0
