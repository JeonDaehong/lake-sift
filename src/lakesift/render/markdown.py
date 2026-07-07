"""Markdown output — a diff summary that renders in a GitHub PR comment or in
`$GITHUB_STEP_SUMMARY`. Same information as the human renderer, laid out for review.
"""

from __future__ import annotations

from lakesift.result import DiffResult

# With no --sample, cap sample rows so a huge diff doesn't bloat a PR comment.
DEFAULT_MAX_ROWS = 20


def _fmt_pairs(d: dict) -> str:
    """Render a dict (a row, or a row's key) as `col=value` pairs."""
    return ", ".join(f"{k}={v!r}" for k, v in d.items())


def _sample(items, total: int, render, max_rows: int) -> list[str]:
    """Collect up to max_rows rendered lines, appending a '… +N more' marker."""
    out: list[str] = []
    for it in items:
        if len(out) >= max_rows:
            out.append(f"… +{total - max_rows} more")
            break
        out.append(render(it))
    return out


def render_markdown(
    result: DiffResult,
    *,
    title: str = "lake-sift",
    max_rows: int = DEFAULT_MAX_ROWS,
    top_columns: int = 5,
    summary_only: bool = False,
) -> str:
    """Render `result` as a GitHub-flavored Markdown report string."""
    lines: list[str] = [f"### {title}", ""]

    if result.is_empty():
        lines.append("✅ **No differences** — the datasets are identical.")
        return "\n".join(lines) + "\n"

    s = result.summary()
    lines += [
        "| Change | Count |",
        "| --- | ---: |",
        f"| ➕ added rows | {s['added']} |",
        f"| ➖ removed rows | {s['removed']} |",
        f"| 🔁 changed rows | {s['changed']} |",
        f"| &nbsp;&nbsp;↳ changed cells | {s['changed_cells']} |",
        f"| 🧬 schema changes | {s['schema_changes']} |",
        "",
    ]

    # Top-K columns by changed cells.
    if top_columns > 0 and result.changed_by_column:
        top = result.changed_by_column[:top_columns]
        parts = ", ".join(f"`{col}` ({n})" for col, n in top)
        rest = len(result.changed_by_column) - len(top)
        if rest > 0:
            parts += f", … +{rest} more"
        lines += [f"**Top changed columns:** {parts}", ""]

    # Schema deltas.
    if result.schema_changes:
        lines.append("**Schema changes**")
        lines.append("")
        for c in result.schema_changes:
            if c.kind == "added":
                lines.append(f"- ➕ `{c.column}` ({c.new_type})")
            elif c.kind == "removed":
                lines.append(f"- ➖ `{c.column}` ({c.old_type})")
            else:
                lines.append(f"- 🔁 `{c.column}`: {c.old_type} → {c.new_type}")
        lines.append("")

    if summary_only:
        return "\n".join(lines).rstrip() + "\n"

    removed = _sample(result.removed, s["removed"], lambda r: f"➖ {_fmt_pairs(r)}", max_rows)
    added = _sample(result.added, s["added"], lambda r: f"➕ {_fmt_pairs(r)}", max_rows)
    changed = _sample(
        result.changed_cells,
        s["changed_cells"],
        lambda c: f"🔁 [{_fmt_pairs(c.key)}] {c.column}: {c.old!r} → {c.new!r}",
        max_rows,
    )

    body = removed + added + changed
    if body:
        # Collapse the row detail so the summary table stays the headline.
        lines.append("<details><summary>Sample changes</summary>")
        lines.append("")
        lines += [f"- {line}" for line in body]
        lines.append("")
        lines.append("</details>")

    return "\n".join(lines).rstrip() + "\n"
