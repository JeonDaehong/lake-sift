"""diff 결과 타입들."""

from __future__ import annotations

import json as _json
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class SchemaChange:
    """스키마 델타 한 건."""

    column: str
    kind: str  # "added" | "removed" | "type_changed"
    old_type: str | None = None
    new_type: str | None = None


@dataclass(frozen=True)
class CellChange:
    """양쪽에 다 있는 key인데 값이 바뀐 셀."""

    key: dict[str, Any]
    column: str
    old: Any
    new: Any


@dataclass
class DiffResult:
    """diff 코어의 출력. CLI/렌더러가 이걸 소비한다."""

    key: list[str]
    schema_changes: list[SchemaChange] = field(default_factory=list)
    added: list[dict[str, Any]] = field(default_factory=list)      # right에만 있는 행
    removed: list[dict[str, Any]] = field(default_factory=list)    # left에만 있는 행
    changed_cells: list[CellChange] = field(default_factory=list)
    changed_rows: int = 0  # 값이 하나라도 바뀐 공통 key 행 수

    def is_empty(self) -> bool:
        """차이가 전혀 없으면 True (CI에서 가장 많이 쓰임)."""
        return (
            not self.schema_changes
            and not self.added
            and not self.removed
            and not self.changed_cells
        )

    def summary(self) -> dict[str, int]:
        return {
            "added": len(self.added),
            "removed": len(self.removed),
            "changed": self.changed_rows,
            "changed_cells": len(self.changed_cells),
            "schema_changes": len(self.schema_changes),
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "summary": self.summary(),
            "schema_changes": [
                {
                    "column": c.column,
                    "kind": c.kind,
                    "old_type": c.old_type,
                    "new_type": c.new_type,
                }
                for c in self.schema_changes
            ],
            "added": self.added,
            "removed": self.removed,
            "changed_cells": [
                {"key": c.key, "column": c.column, "old": c.old, "new": c.new}
                for c in self.changed_cells
            ],
        }

    def to_json(self, *, indent: int | None = 2) -> str:
        return _json.dumps(self.to_dict(), default=str, ensure_ascii=False, indent=indent)
