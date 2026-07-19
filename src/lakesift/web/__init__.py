"""lake-sift web UI — a small run-history dashboard over the diff library.

Optional; installed with `pip install lake-sift[web]` (FastAPI + uvicorn + Jinja2) and
launched with `lake-sift web`. The UI never re-implements comparison logic — it drives the
same `diff`/`schema_diff`/`preview` the CLI does and keeps a history of the runs.
"""

from __future__ import annotations

import os
from pathlib import Path

DEFAULT_PORT = 7438  # "SIFT" on a phone keypad (7-4-3-8); clear of Airflow/Jenkins 8080 et al.


def default_db_path() -> Path:
    """Where run history lives by default: ~/.lake-sift/history.db (override with --db)."""
    root = Path(os.environ.get("LAKESIFT_HOME", Path.home() / ".lake-sift"))
    return root / "history.db"


def create_app(store):
    """Build the FastAPI app around a `RunStore` (kept lazy so the core stays import-light)."""
    from lakesift.web.app import create_app as _create_app

    return _create_app(store)


__all__ = ["DEFAULT_PORT", "default_db_path", "create_app"]
