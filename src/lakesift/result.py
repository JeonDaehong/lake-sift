"""diff 결과 타입들.

`DiffResult` 는 두 가지 방식으로 만들어진다:

1. **eager** — `added`/`removed`/`changed_cells` 에 그냥 시퀀스(list 등)를 넘긴다.
   작은 결과/테스트/라이브러리 단순 사용용. 카운트는 `len()` 으로 자동.
2. **streaming** — `diff()` 가 쓰는 방식. 위 인자에 "호출하면 새 이터레이터를
   돌려주는 무인자 콜러블"(generator factory) 을 넣고, 카운트를 `counts=` 로 명시,
   살아있는 DuckDB 커넥션을 `resource=` 로 넘긴다. 행/셀은 메모리에 전량 적재하지
   않고 접근할 때마다 커서로 흘린다.

streaming 모드에서는 커넥션을 닫아야 하므로 컨텍스트 매니저로 쓰는 걸 권장한다::

    with diff(left, right, key=["id"]) as result:
        print(result.summary())
        for row in result.added:
            ...
"""

from __future__ import annotations

import json as _json
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Iterator, Sequence, Union

# 시퀀스(재이터 가능) 거나, 호출 시 새 이터레이터를 주는 콜러블.
_Rows = Union[Sequence[Any], Callable[[], Iterator[Any]]]


def _fresh(src: _Rows) -> Iterator[Any]:
    """소스가 콜러블이면 호출해 새 이터레이터를, 시퀀스면 iter() 를 돌려준다."""
    return src() if callable(src) else iter(src)


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


class DiffResult:
    """diff 코어의 출력. CLI/렌더러가 이걸 소비한다.

    `added`/`removed`/`changed_cells` 는 **프로퍼티** 로, 접근할 때마다 새 이터레이터를
    돌려준다. streaming 모드에선 매 접근이 커서를 다시 연다(전량 적재 회피). 따라서
    `len()`/인덱싱은 직접 못 쓰고, 개수는 `summary()` 를, 전체 목록은 `list(...)` 를 쓴다.
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
        # 변경 셀 수 내림차순 (col, count). 0 건 컬럼은 제외됨.
        self.changed_by_column: list[tuple[str, int]] = list(changed_by_column or [])
        self._counts = counts or {}
        self._resource = resource  # 소유한 DuckDB 커넥션 (있으면 close 책임짐)

    # --- lazy 접근자 (접근마다 fresh) ---
    @property
    def added(self) -> Iterator[dict[str, Any]]:
        return _fresh(self._added)

    @property
    def removed(self) -> Iterator[dict[str, Any]]:
        return _fresh(self._removed)

    @property
    def changed_cells(self) -> Iterator[CellChange]:
        return _fresh(self._changed_cells)

    # --- 카운트 (eager) ---
    def _count(self, name: str, src: _Rows) -> int:
        if name in self._counts:
            return self._counts[name]
        if callable(src):  # streaming 인데 카운트 누락 → 버그
            raise RuntimeError(f"streaming 소스 '{name}' 의 카운트가 없습니다.")
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
        """차이가 전혀 없으면 True (CI에서 가장 많이 쓰임)."""
        return (
            not self.schema_changes
            and self._count("added", self._added) == 0
            and self._count("removed", self._removed) == 0
            and self._count("changed_cells", self._changed_cells) == 0
        )

    # --- 직렬화 ---
    def to_dict(self) -> dict[str, Any]:
        """전체를 dict 로 materialize. 큰 결과면 메모리를 쓰므로 `write_json` 을 선호."""
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
            "changed_by_column": [
                {"column": col, "count": n} for col, n in self.changed_by_column
            ],
            "added": list(self.added),
            "removed": list(self.removed),
            "changed_cells": [
                {"key": c.key, "column": c.column, "old": c.old, "new": c.new}
                for c in self.changed_cells
            ],
        }

    def to_json(self, *, indent: int | None = 2) -> str:
        return _json.dumps(self.to_dict(), default=str, ensure_ascii=False, indent=indent)

    def write_json(self, stream) -> None:
        """행/셀을 한 건씩 흘려쓰는 스트리밍 JSON 출력 (전량 적재 안 함).

        compact(공백 없는) 형식이다 — 사람이 보기보다 파이프/리다이렉트용.
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
        array(
            self.schema_changes,
            lambda c: {
                "column": c.column,
                "kind": c.kind,
                "old_type": c.old_type,
                "new_type": c.new_type,
            },
        )
        stream.write(',"changed_by_column":')
        array(self.changed_by_column, lambda t: {"column": t[0], "count": t[1]})
        stream.write(',"added":')
        array(self.added, lambda r: r)
        stream.write(',"removed":')
        array(self.removed, lambda r: r)
        stream.write(',"changed_cells":')
        array(
            self.changed_cells,
            lambda c: {"key": c.key, "column": c.column, "old": c.old, "new": c.new},
        )
        stream.write("}")

    # --- 리소스 수명 ---
    def close(self) -> None:
        if self._resource is not None:
            self._resource.close()
            self._resource = None

    def __enter__(self) -> "DiffResult":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __del__(self) -> None:  # pragma: no cover - 안전망
        try:
            self.close()
        except Exception:
            pass
