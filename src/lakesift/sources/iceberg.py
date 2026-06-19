"""Iceberg 소스 어댑터 (v0.3) — PyIceberg 로 스냅샷을 읽는다.

코어/렌더러는 그대로. PyIceberg `Table` 을 받아 scan → Arrow → DuckDB relation 으로
넘긴다. 스캔은 현재 Arrow 로 전량 materialize 한다(단일 노드 도구라 허용) — 큰
테이블은 `row_filter`/`selected_fields` 로 미리 줄여라. diff 출력 자체는 여전히
스트리밍된다.

pyiceberg 는 선택 의존성: `pip install "lake-sift[iceberg]"`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

if TYPE_CHECKING:
    import duckdb
    from pyiceberg.table import Table


def _require_pyiceberg() -> None:
    try:
        import pyiceberg  # noqa: F401
    except ImportError as e:  # pragma: no cover - 설치 안 된 환경에서만
        raise ImportError(
            'Iceberg 소스는 pyiceberg 가 필요합니다: pip install "lake-sift[iceberg]"'
        ) from e


class IcebergSource:
    """PyIceberg `Table` 을 DuckDB relation 으로 읽는다.

    이미 로드한 Table 을 직접 넘기거나(`IcebergSource(table)`), 카탈로그에서
    이름으로 로드한다(`IcebergSource.from_catalog(...)`).
    """

    def __init__(
        self,
        table: "Table",
        *,
        snapshot_id: int | None = None,
        row_filter: Any = None,
        selected_fields: Sequence[str] | None = None,
    ):
        self.table = table
        self.snapshot_id = snapshot_id
        self.row_filter = row_filter
        self.selected_fields = tuple(selected_fields) if selected_fields else ("*",)

    @classmethod
    def from_catalog(
        cls,
        catalog: str,
        identifier: str,
        *,
        snapshot_id: int | None = None,
        row_filter: Any = None,
        selected_fields: Sequence[str] | None = None,
        **properties: Any,
    ) -> "IcebergSource":
        """카탈로그(REST/Glue/SQL 등)에서 `identifier` 테이블을 로드한다.

        `properties` 는 pyiceberg `load_catalog` 로 그대로 전달된다(uri, credential 등).
        """
        _require_pyiceberg()
        from pyiceberg.catalog import load_catalog

        tbl = load_catalog(catalog, **properties).load_table(identifier)
        return cls(
            tbl,
            snapshot_id=snapshot_id,
            row_filter=row_filter,
            selected_fields=selected_fields,
        )

    def to_relation(self, con: "duckdb.DuckDBPyConnection") -> "duckdb.DuckDBPyRelation":
        _require_pyiceberg()
        kwargs: dict[str, Any] = {"selected_fields": self.selected_fields}
        if self.snapshot_id is not None:
            kwargs["snapshot_id"] = self.snapshot_id
        if self.row_filter is not None:  # None 이면 scan 기본값(ALWAYS_TRUE) 사용
            kwargs["row_filter"] = self.row_filter
        arrow = self.table.scan(**kwargs).to_arrow()
        return con.from_arrow(arrow)

    def __repr__(self) -> str:  # pragma: no cover
        return f"IcebergSource({self.table!r})"
