"""diff result types.

`DiffResult` can be built in two ways:

1. **eager** — pass plain sequences (lists, etc.) for `added`/`removed`/`changed_cells`.
   For small results, tests, and simple library use. Counts come from `len()`.
2. **streaming** — what `diff()` uses. Pass a zero-argument callable that returns a
   fresh iterator (a generator factory) for those arguments, give the counts via
   `counts=`, and hand the live DuckDB connection via `resource=`. Rows/cells are not
   materialized in memory; each access streams them through a cursor.

In streaming mode the connection must be closed, so using it as a context manager is
recommended::

    with diff(left, right, key=["id"]) as result:
        print(result.summary())
        for row in result.added:
            ...
"""

from __future__ import annotations

import json as _json
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Iterator, Sequence, Union

# Either a (re-iterable) sequence, or a callable that returns a fresh iterator.
_Rows = Union[Sequence[Any], Callable[[], Iterator[Any]]]


def _fresh(src: _Rows) -> Iterator[Any]:
    """Call the source if it is a callable (fresh iterator); otherwise iter() it."""
    return src() if callable(src) else iter(src)


# --- JSON object shapes (shared by to_dict and the streaming write_json) ---
def _schema_change_obj(c: "SchemaChange") -> dict[str, Any]:
    return {"column": c.column, "kind": c.kind, "old_type": c.old_type, "new_type": c.new_type}


def _cell_obj(c: "CellChange") -> dict[str, Any]:
    return {"key": c.key, "column": c.column, "old": c.old, "new": c.new}


def _col_count_obj(t: tuple[str, int]) -> dict[str, Any]:
    return {"column": t[0], "count": t[1]}


@dataclass(frozen=True)
class SchemaChange:
    """A single schema delta."""

    column: str
    kind: str  # "added" | "removed" | "type_changed"
    old_type: str | None = None
    new_type: str | None = None


@dataclass(frozen=True)
class CellChange:
    """A cell whose key exists on both sides but whose value changed."""

    key: dict[str, Any]
    column: str
    old: Any
    new: Any


class DiffResult:
    """Output of the diff core. Consumed by the CLI/renderers.

    `added`/`removed`/`changed_cells` are **properties** that return a fresh iterator
    on every access. In streaming mode each access reopens a cursor (avoiding full
    materialization). Therefore `len()`/indexing cannot be used directly: use
    `summary()` for counts and `list(...)` for the full list.
    """

    def __init__(
        self,
        key: list[str],
        schema_changes: Sequence[SchemaChange] | None = None,
        added: _Rows = (),
        removed: _Rows = (),
        changed_cells: _Rows = (),
        changed_rows: int = 0,
        *,
        changed_by_column: Sequence[tuple[str, int]] | None = None,
        counts: dict[str, int] | None = None,
        resource: Any = None,
    ) -> None:
        self.key = list(key)
        self.schema_changes: list[SchemaChange] = list(schema_changes or [])
        self._added = added
        self._removed = removed
        self._changed_cells = changed_cells
        self.changed_rows = changed_rows
        # Changed-cell counts descending (col, count). Columns with 0 are excluded.
        self.changed_by_column: list[tuple[str, int]] = list(changed_by_column or [])
        self._counts = counts or {}
        self._resource = resource  # owned DuckDB connection (responsible for closing it)

    # --- lazy accessors (fresh on every access) ---
    @property
    def added(self) -> Iterator[dict[str, Any]]:
        return _fresh(self._added)

    @property
    def removed(self) -> Iterator[dict[str, Any]]:
        return _fresh(self._removed)

    @property
    def changed_cells(self) -> Iterator[CellChange]:
        return _fresh(self._changed_cells)

    # --- counts (eager) ---
    def _count(self, name: str, src: _Rows) -> int:
        if name in self._counts:
            return self._counts[name]
        if callable(src):  # streaming source with a missing count -> bug
            raise RuntimeError(f"streaming source '{name}' has no count.")
        return len(src)

    def summary(self) -> dict[str, int]:
        return {
            "added": self._count("added", self._added),
            "removed": self._count("removed", self._removed),
            "changed": self.changed_rows,
            "changed_cells": self._count("changed_cells", self._changed_cells),
            "schema_changes": len(self.schema_changes),
        }

    def is_empty(self) -> bool:
        """True when there is no difference at all (the most common CI check)."""
        return (
            not self.schema_changes
            and self._count("added", self._added) == 0
            and self._count("removed", self._removed) == 0
            and self._count("changed_cells", self._changed_cells) == 0
        )

    # --- serialization ---
    def to_dict(self) -> dict[str, Any]:
        """Materialize everything into a dict. Prefer `write_json` for large results."""
        return {
            "key": self.key,
            "summary": self.summary(),
            "schema_changes": [_schema_change_obj(c) for c in self.schema_changes],
            "changed_by_column": [_col_count_obj(t) for t in self.changed_by_column],
            "added": list(self.added),
            "removed": list(self.removed),
            "changed_cells": [_cell_obj(c) for c in self.changed_cells],
        }

    def to_json(self, *, indent: int | None = 2) -> str:
        return _json.dumps(self.to_dict(), default=str, ensure_ascii=False, indent=indent)

    def write_json(self, stream) -> None:
        """Stream the JSON output one row/cell at a time (no full materialization).

        Compact (no-whitespace) format — meant for pipes/redirection rather than
        for humans to read.
        """

        def dump(obj: Any) -> str:
            return _json.dumps(obj, default=str, ensure_ascii=False, separators=(",", ":"))

        def array(items: Iterable[Any], to_obj: Callable[[Any], Any]) -> None:
            stream.write("[")
            for i, it in enumerate(items):
                if i:
                    stream.write(",")
                stream.write(dump(to_obj(it)))
            stream.write("]")

        stream.write("{")
        stream.write('"key":' + dump(self.key))
        stream.write(',"summary":' + dump(self.summary()))
        stream.write(',"schema_changes":')
        array(self.schema_changes, _schema_change_obj)
        stream.write(',"changed_by_column":')
        array(self.changed_by_column, _col_count_obj)
        stream.write(',"added":')
        array(self.added, lambda r: r)
        stream.write(',"removed":')
        array(self.removed, lambda r: r)
        stream.write(',"changed_cells":')
        array(self.changed_cells, _cell_obj)
        stream.write("}")

    # --- resource lifetime ---
    def close(self) -> None:
        if self._resource is not None:
            self._resource.close()
            self._resource = None

    def __enter__(self) -> "DiffResult":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __del__(self) -> None:  # pragma: no cover - safety net
        try:
            self.close()
        except Exception:
            pass
