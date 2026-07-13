"""Helpers shared by the human and Markdown renderers."""

from __future__ import annotations

from typing import Any, Iterable, Iterator, Sequence, Tuple

# With no --sample, cap the sample rows so a huge diff doesn't flood the console
# or bloat a PR comment. Shared so both renderers default identically.
DEFAULT_MAX_ROWS = 20


def fmt_pairs(d: dict) -> str:
    """Render a dict (a row, or a row's key) as `col=value` pairs."""
    return ", ".join(f"{k}={v!r}" for k, v in d.items())


def top_split(
    changed_by_column: Sequence[Tuple[str, int]], k: int
) -> Tuple[Sequence[Tuple[str, int]], int]:
    """Split the changed-by-column list for the 'top changed columns' line.

    Returns `(top_k, remaining)` — the first `k` (col, count) pairs and how many were
    left out. Centralizes the slice + overflow count both renderers need.
    """
    top = changed_by_column[:k]
    return top, len(changed_by_column) - len(top)


def sampled(items: Iterable[Any], total: int, max_rows: int) -> Iterator[Tuple[str, Any]]:
    """Stream a change list capped at `max_rows`, tagging each yield for the caller.

    Yields `("row", item)` for up to `max_rows` items, then `("more", overflow)` once if
    the source has more, where `overflow = total - max_rows`. This keeps the truncation
    and overflow-count logic in one place; each renderer only decides how to format a row
    line versus the "… +N more" marker. `items` may be a streaming iterator, so it is
    never materialized.
    """
    shown = 0
    for it in items:
        if shown >= max_rows:
            yield "more", total - max_rows
            return
        yield "row", it
        shown += 1
