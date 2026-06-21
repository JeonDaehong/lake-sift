"""Delta Lake 소스 어댑터 (v0.4) — delta-rs 로 테이블을 읽는다.

코어/렌더러는 그대로. delta-rs `DeltaTable` 을 받아 Arrow 로 materialize → DuckDB
relation 으로 넘긴다. Iceberg 어댑터와 동형이다(스캔은 Arrow 로 전량 적재 — 단일
노드 도구라 허용. 큰 테이블은 `columns`/`filters` 로 미리 줄여라). diff 출력 자체는
여전히 스트리밍된다.

deltalake 는 선택 의존성: `pip install "lake-sift[delta]"`.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Sequence

if TYPE_CHECKING:
    import duckdb
    from deltalake import DeltaTable


def _require_deltalake() -> None:
    try:
        import deltalake  # noqa: F401
    except ImportError as e:  # pragma: no cover - 설치 안 된 환경에서만
        raise ImportError(
            'Delta 소스는 deltalake 가 필요합니다: pip install "lake-sift[delta]"'
        ) from e


class DeltaSource:
    """delta-rs 로 Delta Lake 테이블을 DuckDB relation 으로 읽는다.

    경로/URI 로 열거나(`DeltaSource("/path/to/table")`, `DeltaSource("s3://bucket/t")`),
    이미 로드한 `DeltaTable` 을 직접 넘긴다(`DeltaSource(dt)`). `version` 으로 특정
    버전 타임트래블, `storage_options` 로 원격 스토리지 자격증명을 전달한다.
    """

    def __init__(
        self,
        table: "DeltaTable | str | os.PathLike[str]",
        *,
        version: int | None = None,
        storage_options: dict[str, str] | None = None,
        columns: Sequence[str] | None = None,
        filters: Any = None,
    ):
        self.table = table
        self.version = version
        self.storage_options = storage_options
        self.columns = list(columns) if columns else None
        self.filters = filters

    def _load(self) -> "DeltaTable":
        _require_deltalake()
        from deltalake import DeltaTable

        if isinstance(self.table, DeltaTable):
            dt = self.table
            if self.version is not None:  # 넘겨받은 객체를 요청 버전으로 이동시킨다
                dt.load_as_version(self.version)
            return dt
        return DeltaTable(
            os.fspath(self.table),
            version=self.version,
            storage_options=self.storage_options,
        )

    def to_relation(self, con: "duckdb.DuckDBPyConnection") -> "duckdb.DuckDBPyRelation":
        dt = self._load()
        arrow = dt.to_pyarrow_table(columns=self.columns, filters=self.filters)
        return con.from_arrow(arrow)

    def __repr__(self) -> str:  # pragma: no cover
        return f"DeltaSource({self.table!r})"
