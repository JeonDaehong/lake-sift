"""markdown renderer output."""

from __future__ import annotations

from lakesift.render.markdown import render_markdown
from lakesift.result import CellChange, DiffResult, SchemaChange


def test_empty_result():
    out = render_markdown(DiffResult(key=["id"]))
    assert "No differences" in out
    assert out.startswith("### lake-sift")


def test_summary_table_and_counts():
    result = DiffResult(
        key=["id"],
        added=[{"id": 4, "v": "d"}],
        removed=[{"id": 1, "v": "a"}],
        changed_cells=[CellChange(key={"id": 3}, column="v", old="c", new="C")],
        changed_rows=1,
        changed_by_column=[("v", 1)],
    )
    out = render_markdown(result)
    # a GitHub-renderable table plus a headline for the shifted column
    assert "| Change | Count |" in out
    assert "changed cells" in out
    assert "Top changed columns" in out and "`v` (1)" in out
    # sample rows are collapsed but present
    assert "<details><summary>Sample changes</summary>" in out
    assert "[id=3] v: 'c' → 'C'" in out


def test_schema_changes_rendered():
    result = DiffResult(
        key=["id"],
        schema_changes=[
            SchemaChange(column="age", kind="added", new_type="INTEGER"),
            SchemaChange(column="old", kind="removed", old_type="VARCHAR"),
        ],
    )
    out = render_markdown(result)
    assert "Schema changes" in out
    assert "`age` (INTEGER)" in out
    assert "`old` (VARCHAR)" in out


def test_summary_only_omits_sample():
    result = DiffResult(
        key=["id"],
        added=[{"id": 4}],
        changed_rows=0,
    )
    out = render_markdown(result, summary_only=True)
    assert "| Change | Count |" in out
    assert "Sample changes" not in out


def test_max_rows_caps_sample():
    rows = [{"id": i} for i in range(50)]
    result = DiffResult(key=["id"], added=rows)
    out = render_markdown(result, max_rows=5)
    assert "… +45 more" in out
