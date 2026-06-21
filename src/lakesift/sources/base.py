"""Source 인터페이스 — 입력을 (DuckDB relation + schema) 로 해석한다.

어댑터만 갈아끼우면 입력 포맷이 늘어난다. 코어/렌더러는 그대로.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, Sequence, runtime_checkable

if TYPE_CHECKING:
    import duckdb


@runtime_checkable
class Source(Protocol):
    """모든 입력 소스가 구현해야 하는 최소 계약.

    선택적으로 `arrow_schema() -> pyarrow.Schema` 를 구현하면, 코어가 컬럼
    projection(pushdown) 을 결정하기 전에 데이터를 읽지 않고 스키마만 싸게
    가져온다(Iceberg/Delta 처럼 `to_relation` 이 전량 materialize 하는 소스에서
    유효). 미구현이면 코어는 `to_relation` 의 relation 에서 스키마를 읽는다.
    """

    def to_relation(
        self,
        con: "duckdb.DuckDBPyConnection",
        *,
        columns: Sequence[str] | None = None,
    ) -> "duckdb.DuckDBPyRelation":
        """이 소스를 DuckDB relation 으로 만든다.

        `columns` 가 주어지면 그 컬럼만 읽는다(스캔에 pushdown). `None` 이면 전체.
        """
        ...
