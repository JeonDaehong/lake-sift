"""The FastAPI application: routes + templating over the run store.

Kept framework-thin. A POST hands a `RunRequest` to a background worker (so the page can
show a live 'running' badge, Jenkins-style), everything else just reads the store and
renders a template. No comparison logic lives here — see `runner`.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from lakesift import __version__
from lakesift.web import runner
from lakesift.web.runner import RunRequest
from lakesift.web.store import RunStore

_TEMPLATES = Path(__file__).parent / "templates"

# Badge text + CSS class per stored status. Colour lives in the stylesheet (base.html).
_STATUS = {
    "running": ("running", "st-running"),
    "identical": ("identical", "st-ok"),
    "differences": ("differences", "st-diff"),
    "schema": ("schema change", "st-schema"),
    "error": ("error", "st-error"),
}
_KIND_LABEL = {"diff": "Diff", "schema": "Schema", "preview": "Preview"}


def _split_csv(value: Optional[str]) -> list[str]:
    if not value:
        return []
    return [p.strip() for p in value.split(",") if p.strip()]


def _split_lines(value: Optional[str]) -> list[str]:
    if not value:
        return []
    return [ln.strip() for ln in value.splitlines() if ln.strip()]


def _fmt_ago(created_at: float) -> str:
    delta = max(0, int(time.time() - created_at))
    if delta < 60:
        return f"{delta}s ago"
    if delta < 3600:
        return f"{delta // 60}m ago"
    if delta < 86400:
        return f"{delta // 3600}h ago"
    return f"{delta // 86400}d ago"


def _fmt_dur(ms: Optional[int]) -> str:
    if ms is None:
        return "—"
    if ms < 1000:
        return f"{ms} ms"
    return f"{ms / 1000:.1f} s"


def _fmt_when(created_at: float) -> str:
    """Absolute local time, for a tooltip alongside the relative 'x ago'."""
    return datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S")


def _comma(n) -> str:
    try:
        return f"{int(n):,}"
    except (TypeError, ValueError):
        return "0" if n is None else str(n)


def create_app(store: RunStore) -> FastAPI:
    app = FastAPI(title="lake-sift", docs_url=None, redoc_url=None)
    app.state.store = store
    app.state.pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="lakesift-run")

    # Prefill values come either as lists (stored params, for a re-run) or as raw strings
    # (a rejected form we're re-rendering). These filters accept both.
    def _csv(v):
        return ", ".join(v) if isinstance(v, (list, tuple)) else (v or "")

    def _lines(v):
        return "\n".join(v) if isinstance(v, (list, tuple)) else (v or "")

    templates = Jinja2Templates(directory=str(_TEMPLATES))
    templates.env.filters["ago"] = _fmt_ago
    templates.env.filters["dur"] = _fmt_dur
    templates.env.filters["when"] = _fmt_when
    templates.env.filters["comma"] = _comma
    templates.env.filters["csv"] = _csv
    templates.env.filters["lines"] = _lines
    templates.env.globals["version"] = __version__
    templates.env.globals["status_label"] = lambda s: _STATUS.get(s, (s, "st-error"))[0]
    templates.env.globals["status_class"] = lambda s: _STATUS.get(s, (s, "st-error"))[1]
    templates.env.globals["kind_label"] = lambda k: _KIND_LABEL.get(k, k)

    def _submit(req: RunRequest) -> int:
        run_id = store.create(
            kind=req.mode,
            left=req.left,
            right=req.right,
            key=", ".join(req.key) or None,
            source_kind=runner.pair_kind(req.left, req.right),
            params=req.to_params(),
        )
        app.state.pool.submit(runner.execute, store, run_id, req)
        return run_id

    @app.get("/", include_in_schema=False)
    def index() -> RedirectResponse:
        return RedirectResponse("/runs", status_code=303)

    @app.get("/runs", response_class=HTMLResponse, include_in_schema=False)
    def runs(request: Request, kind: Optional[str] = None):
        rows = store.list(kind=kind)
        return templates.TemplateResponse(
            request,
            "runs.html",
            {
                "runs": rows,
                "kind": kind,
                "counts": store.counts_by_status(),
                "active": "runs",
            },
        )

    @app.get("/runs/new", response_class=HTMLResponse, include_in_schema=False)
    def new_run(request: Request, from_: Optional[int] = None):
        prefill = None
        if from_ is not None:
            existing = store.get(from_)
            if existing is not None:
                prefill = existing["params"]
        return templates.TemplateResponse(
            request,
            "new_run.html",
            {"prefill": prefill, "error": None, "active": "new"},
        )

    @app.post("/runs", include_in_schema=False)
    def create_run(
        request: Request,
        mode: str = Form("diff"),
        left: str = Form(""),
        right: str = Form(""),
        key: str = Form(""),
        exclude: str = Form(""),
        columns: str = Form(""),
        tolerance: str = Form(""),
        ignore_case: bool = Form(False),
        allow_duplicates: bool = Form(False),
        structural_only: bool = Form(False),
        upstreams: str = Form(""),
        sql_dialect: str = Form("duckdb"),
    ):
        keys = _split_csv(key)
        error = None
        tol: Optional[float] = None
        if not left.strip() or not right.strip():
            error = "Both a left (before) and a right (after) source are required."
        elif mode == "diff" and not keys:
            error = "A key is required for a value diff (use Schema or Preview mode for a keyless check)."
        elif tolerance.strip():
            try:
                tol = float(tolerance)
            except ValueError:
                error = f"Tolerance must be a number: {tolerance!r}"

        if error is not None:
            form = {
                "mode": mode, "left": left, "right": right, "key": key,
                "exclude": exclude, "columns": columns, "tolerance": tolerance,
                "ignore_case": ignore_case, "allow_duplicates": allow_duplicates,
                "structural_only": structural_only, "upstreams": upstreams,
                "sql_dialect": sql_dialect,
            }
            return templates.TemplateResponse(
                request,
                "new_run.html",
                {"prefill": form, "error": error, "active": "new"},
                status_code=400,
            )

        req = RunRequest(
            mode=mode,
            left=left.strip(),
            right=right.strip(),
            key=keys,
            exclude=_split_csv(exclude),
            columns=_split_csv(columns),
            tolerance=tol,
            ignore_case=ignore_case,
            allow_duplicates=allow_duplicates,
            structural_only=structural_only,
            upstreams=_split_lines(upstreams),
            sql_dialect=sql_dialect.strip() or "duckdb",
        )
        run_id = _submit(req)
        return RedirectResponse(f"/runs/{run_id}", status_code=303)

    @app.post("/runs/{run_id}/rerun", include_in_schema=False)
    def rerun(run_id: int):
        existing = store.get(run_id)
        if existing is None or not existing.get("params"):
            return RedirectResponse("/runs", status_code=303)
        new_id = _submit(RunRequest.from_params(existing["params"]))
        return RedirectResponse(f"/runs/{new_id}", status_code=303)

    @app.post("/runs/{run_id}/delete", include_in_schema=False)
    def delete_run(run_id: int):
        store.delete(run_id)
        return RedirectResponse("/runs", status_code=303)

    @app.get("/runs/{run_id}", response_class=HTMLResponse, include_in_schema=False)
    def run_detail(request: Request, run_id: int):
        run = store.get(run_id)
        if run is None:
            return templates.TemplateResponse(
                request, "not_found.html", {"run_id": run_id}, status_code=404
            )
        return templates.TemplateResponse(
            request, "run_detail.html", {"run": run, "active": "runs"}
        )

    @app.get("/runs/{run_id}/json", include_in_schema=False)
    def run_json(run_id: int):
        run = store.get(run_id)
        if run is None:
            return JSONResponse({"error": "not found"}, status_code=404)
        return JSONResponse(run)

    @app.get("/environment", response_class=HTMLResponse, include_in_schema=False)
    def environment(request: Request):
        from lakesift.web.environment import probe_environment

        return templates.TemplateResponse(
            request,
            "environment.html",
            {"env": probe_environment(store.path), "active": "env"},
        )

    return app
