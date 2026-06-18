"""Source 인터페이스 — 입력을 (DuckDB relation + schema) 로 해석한다.

어댑터만 갈아끼우면 입력 포맷이 늘어난다. 코어/렌더러는 그대로.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    import duckdb


@runtime_checkable
class Source(Protocol):
    """모든 입력 소스가 구현해야 하는 최소 계약."""

    def to_relation(self, con: "duckdb.DuckDBPyConnection") -> "duckdb.DuckDBPyRelation":
        """주어진 DuckDB 연결 위에서 이 소스를 relation 으로 만든다."""
        ...
