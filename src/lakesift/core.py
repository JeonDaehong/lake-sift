"""diff 엔진 — key/옵션을 받아 DuckDB SQL 로 비교한다.

Python 은 SQL 을 생성/오케스트레이션만 하고, 무거운 비교는 전부 DuckDB 에 위임한다.
NULL 동등 비교(`NULL == NULL` 을 같음으로) 는 SQL 의 `IS [NOT] DISTINCT FROM` 으로 처리.
float 는 v0 에서 정확 일치(exact) 비교 — 수치 tolerance 는 v0.2.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

import duckdb

from lakesift.result import CellChange, DiffResult, SchemaChange

if TYPE_CHECKING:
    from lakesift.sources.base import Source


class DiffError(Exception):
    """비교를 진행할 수 없는 에러 (CLI 에서 exit code 2 로 매핑)."""


def _q(name: str) -> str:
    """식별자를 안전하게 따옴표로 감싼다."""
    return '"' + name.replace('"', '""') + '"'


def _schema_of(rel: "duckdb.DuckDBPyRelation") -> dict[str, str]:
    """relation 의 컬럼명 → 타입 문자열 (입력 순서 유지)."""
    return {name: str(t) for name, t in zip(rel.columns, rel.types)}


def _has_duplicate_keys(con, view: str, key: Sequence[str]) -> bool:
    cols = ", ".join(_q(k) for k in key)
    sql = f"SELECT 1 FROM {view} GROUP BY {cols} HAVING count(*) > 1 LIMIT 1"
    return con.execute(sql).fetchone() is not None


def _fetch_dicts(con, sql: str) -> list[dict[str, Any]]:
    res = con.execute(sql)
    cols = [d[0] for d in res.description]
    return [dict(zip(cols, row)) for row in res.fetchall()]


def diff(
    left: "Source",
    right: "Source",
    key: Sequence[str],
    *,
    exclude: Sequence[str] | None = None,
    columns: Sequence[str] | None = None,
    allow_duplicates: bool = False,
) -> DiffResult:
    """left 와 right 를 key 기준으로 셀 단위 비교한다.

    Raises:
        DiffError: key 누락/부재, 중복 key(allow_duplicates=False) 등 비교 불가 상황.
    """
    key = list(key)
    if not key:
        # v0: set-diff 폴백은 미구현. 명시적 에러로 끊는다.
        raise DiffError("key 가 필요합니다 (set-diff 폴백은 아직 미지원).")

    exclude_set = set(exclude or [])
    columns_filter = set(columns) if columns else None

    con = duckdb.connect()  # in-memory
    try:
        lrel = left.to_relation(con)
        rrel = right.to_relation(con)
        lschema = _schema_of(lrel)
        rschema = _schema_of(rrel)
        lrel.create_view("l", replace=True)
        rrel.create_view("r", replace=True)

        # --- key 검증 ---
        missing = [k for k in key if k not in lschema or k not in rschema]
        if missing:
            raise DiffError(f"key 컬럼이 양쪽에 모두 존재하지 않습니다: {missing}")

        # --- 스키마 델타 ---
        schema_changes: list[SchemaChange] = []
        for col, t in lschema.items():
            if col not in rschema:
                schema_changes.append(SchemaChange(col, "removed", old_type=t))
            elif rschema[col] != t:
                schema_changes.append(
                    SchemaChange(col, "type_changed", old_type=t, new_type=rschema[col])
                )
        for col, t in rschema.items():
            if col not in lschema:
                schema_changes.append(SchemaChange(col, "added", new_type=t))

        # --- 중복 key ---
        if not allow_duplicates:
            for view, label in (("l", "left"), ("r", "right")):
                if _has_duplicate_keys(con, view, key):
                    raise DiffError(
                        f"{label} 에 중복 key 가 있습니다. --allow-duplicates 로 우회 가능."
                    )

        # --- 비교 대상 컬럼: 공통 컬럼 - key - exclude (columns 지정 시 그걸로 제한) ---
        common = [c for c in lschema if c in rschema]
        compare_cols = [
            c
            for c in common
            if c not in key
            and c not in exclude_set
            and (columns_filter is None or c in columns_filter)
        ]

        key_join = " AND ".join(f"l.{_q(k)} IS NOT DISTINCT FROM r.{_q(k)}" for k in key)

        # --- 행 델타 (anti join) ---
        removed = _fetch_dicts(con, f"SELECT l.* FROM l ANTI JOIN r ON {key_join}")
        added = _fetch_dicts(con, f"SELECT r.* FROM r ANTI JOIN l ON {key_join}")

        # --- 셀 델타 + 변경 행 수 ---
        changed_cells: list[CellChange] = []
        changed_rows = 0
        if compare_cols:
            key_sel = ", ".join(f"l.{_q(k)} AS {_q(k)}" for k in key)
            for c in compare_cols:
                qc = _q(c)
                rows = con.execute(
                    f"SELECT {key_sel}, l.{qc} AS old_val, r.{qc} AS new_val "
                    f"FROM l JOIN r ON {key_join} "
                    f"WHERE l.{qc} IS DISTINCT FROM r.{qc}"
                ).fetchall()
                for row in rows:
                    keyvals = {k: row[i] for i, k in enumerate(key)}
                    changed_cells.append(
                        CellChange(key=keyvals, column=c, old=row[-2], new=row[-1])
                    )

            any_diff = " OR ".join(
                f"l.{_q(c)} IS DISTINCT FROM r.{_q(c)}" for c in compare_cols
            )
            changed_rows = con.execute(
                f"SELECT count(*) FROM l JOIN r ON {key_join} WHERE {any_diff}"
            ).fetchone()[0]

        return DiffResult(
            key=key,
            schema_changes=schema_changes,
            added=added,
            removed=removed,
            changed_cells=changed_cells,
            changed_rows=changed_rows,
        )
    finally:
        con.close()
