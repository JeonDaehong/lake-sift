"""`lake-sift web` — parse the web-server flags and run uvicorn.

Invoked from the console entry point (see `lakesift.cli.main_entry`), not through Typer,
so that the existing `lake-sift <left> <right>` diff invocation keeps working untouched.
"""

from __future__ import annotations

import argparse
import sys
from typing import Optional, Sequence

from lakesift import __version__
from lakesift.web import DEFAULT_PORT, default_db_path

_MISSING = (
    "The web UI needs extra packages. Install them with:\n\n"
    "    pip install 'lake-sift[web]'\n"
)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="lake-sift web",
        description="Launch the lake-sift web UI (run-history dashboard).",
    )
    p.add_argument("--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1)")
    p.add_argument(
        "--port", type=int, default=DEFAULT_PORT,
        help=f"Port (default: {DEFAULT_PORT} - 'SIFT' on a phone keypad)",
    )
    p.add_argument(
        "--db", default=None,
        help="SQLite run-history file (default: ~/.lake-sift/history.db)",
    )
    return p


def run(argv: Optional[Sequence[str]] = None) -> None:
    args = _build_parser().parse_args(argv)

    try:
        import uvicorn  # noqa: F401
        from lakesift.web.app import create_app
        from lakesift.web.store import RunStore
    except ModuleNotFoundError:
        sys.stderr.write(_MISSING)
        raise SystemExit(2)

    db_path = args.db or str(default_db_path())
    store = RunStore(db_path)
    app = create_app(store)

    sys.stderr.write(
        f"lake-sift {__version__} - web UI\n"
        f"  history:  {db_path}\n"
        f"  serving:  http://{args.host}:{args.port}\n"
    )
    import uvicorn

    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":  # pragma: no cover
    run()
