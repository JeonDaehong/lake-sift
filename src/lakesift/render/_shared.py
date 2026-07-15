"""Helpers shared by the human and Markdown renderers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable, Iterator, Sequence, Tuple

from lakesift.result import SchemaChange

if TYPE_CHECKING:
    from lakesift.preview import PreviewResult

# With no --sample, cap the sample rows so a huge diff doesn't flood the console
# or bloat a PR comment. Shared so both renderers default identically.
DEFAULT_MAX_ROWS = 20

# Partitions are listed inline; past this many, show a count instead of a wall of values.
_MAX_PARTITIONS_SHOWN = 5


def fmt_bytes(n: int) -> str:
    """Byte count as a short human-readable size (1708 -> '1.7 KB')."""
    size = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"  # pragma: no cover - unreachable, the loop exits at TB


def fmt_partitions(partitions: Sequence[dict]) -> str:
    """Partition values as `col=value` groups (`dt=2026-07-15, dt=2026-07-14`)."""
    shown = [fmt_pairs(p) for p in partitions[:_MAX_PARTITIONS_SHOWN]]
    rest = len(partitions) - len(shown)
    return ", ".join(shown) + (f", … +{rest} more" if rest > 0 else "")


def preview_facts(p: "PreviewResult") -> list[Tuple[str, str, str]]:
    """The preview's cost table as (label, value, note) rows, shared by both renderers.

    Only the layout differs between human and Markdown output, so the wording — and the
    arithmetic behind it — lives here once.
    """
    facts = [
        (
            "files",
            f"{p.files_differing} of {p.files_left + p.files_right} differ",
            f"{p.files_shared} shared → provably identical",
        ),
        (
            "rows to scan",
            f"{p.rows_to_scan:,} of {p.rows_total:,}",
            f"{p.scan_fraction:.1%} of a full diff",
        ),
        ("bytes to scan", f"{fmt_bytes(p.bytes_to_scan)} of {fmt_bytes(p.bytes_total)}", ""),
    ]
    if p.partitions_touched:
        facts.append(
            ("partitions", f"{len(p.partitions_touched)} touched", fmt_partitions(p.partitions_touched))
        )
    return facts


def fmt_rows(n: int) -> str:
    return f"{n:,} row" if n == 1 else f"{n:,} rows"


def preview_bounds(p: "PreviewResult") -> list[Tuple[str, str, str]]:
    """The key-range proofs as (label, value, note) rows. Empty when no key was given."""
    if not p.key_pruned:
        return []
    note = "pure append/delete: no existing row is touched" if p.is_pure_append() else ""
    return [
        ("provably added", fmt_rows(p.provably_added), ""),
        ("provably removed", fmt_rows(p.provably_removed), ""),
        ("may have changed", f"at most {fmt_rows(p.max_changed_rows)}", note),
    ]


def fmt_pairs(d: dict) -> str:
    """Render a dict (a row, or a row's key) as `col=value` pairs."""
    return ", ".join(f"{k}={v!r}" for k, v in d.items())


def schema_detail(c: SchemaChange) -> str:
    """Type trailer for a schema change, including its leading separator.

    ` (type)` for an added/removed column, `: old → new` for a type change. Both
    renderers append this after the (differently-styled) symbol + column name, so the
    per-kind dispatch lives here rather than being duplicated in each renderer.
    """
    if c.kind == "added":
        return f" ({c.new_type})"
    if c.kind == "removed":
        return f" ({c.old_type})"
    return f": {c.old_type} → {c.new_type}"


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
