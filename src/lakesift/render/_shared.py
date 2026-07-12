"""Helpers shared by the human and Markdown renderers."""

from __future__ import annotations

# With no --sample, cap the sample rows so a huge diff doesn't flood the console
# or bloat a PR comment. Shared so both renderers default identically.
DEFAULT_MAX_ROWS = 20


def fmt_pairs(d: dict) -> str:
    """Render a dict (a row, or a row's key) as `col=value` pairs."""
    return ", ".join(f"{k}={v!r}" for k, v in d.items())
