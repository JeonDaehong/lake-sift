"""diff 엔진 — key/옵션을 받아 DuckDB SQL 로 비교한다.

Python 은 SQL 을 생성/오케스트레이션만 하고, 무거운 비교는 전부 DuckDB 에 위임한다.
NULL 동등 비교(`NULL == NULL` 을 같음으로) 는 SQL 의 `IS [NOT] DISTINCT FROM` 으로 처리.
float 는 v0 에서 정확 일치(exact) 비교 — 수치 tolerance 는 v0.2.

메모리: 행/셀 델타는 파이썬 list 로 전량 적재하지 않는다. 카운트는 집계 쿼리로 먼저
구하고, 실제 행/셀은 `DiffResult` 가 접근할 때 DuckDB 커서로 배치 스트리밍한다.
이 때문에 결과는 살아있는 커넥션을 소유하므로 컨텍스트 매니저로 쓰는 게 안전하다.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterator, Sequence

import duckdb

from lakesift.result import CellChange, DiffResult, SchemaChange

if TYPE_CHECKING:
    from lakesift.sources.base import Source

# 커서에서 한 번에 끌어오는 행 수. 너무 작으면 왕복 비용, 너무 크면 메모리.
_BATCH = 2048


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


# DuckDB 타입 문자열 분류 (대소문자/파라미터 무시).
_NUMERIC_HINTS = ("INT", "DECIMAL", "NUMERIC", "DOUBLE", "FLOAT", "REAL", "HUGEINT")
_TEXT_HINTS = ("VARCHAR", "CHAR", "TEXT", "STRING")


def _is_numeric(t: str) -> bool:
    u = t.upper()
    return any(h in u for h in _NUMERIC_HINTS)


def _is_text(t: str) -> bool:
    u = t.upper()
    return any(h in u for h in _TEXT_HINTS)


def _diff_pred(col: str, ltype: str, *, tolerance: float | None, ignore_case: bool) -> str:
    """셀이 '다르다'고 볼 때 TRUE 인 SQL 불리언 식.

    기본은 `IS DISTINCT FROM` (NULL==NULL 동일). 옵션:
    - tolerance: 수치 컬럼은 `abs(l-r) <= tol` 이면 같음으로 본다.
    - ignore_case: 문자열 컬럼은 대소문자 무시 비교.
    컬럼 타입(왼쪽 기준)에 맞는 한 가지만 적용된다.
    """
    lc, rc = f"l.{_q(col)}", f"r.{_q(col)}"
    if tolerance is not None and _is_numeric(ltype):
        tol = repr(float(tolerance))  # float 이므로 인라인해도 인젝션 안전
        # 다름 = NOT(둘 다 NULL 이거나, 둘 다 non-NULL 이고 tol 이내)
        return (
            f"NOT ( ({lc} IS NULL AND {rc} IS NULL) OR "
            f"({lc} IS NOT NULL AND {rc} IS NOT NULL AND abs({lc} - {rc}) <= {tol}) )"
        )
    if ignore_case and _is_text(ltype):
        return f"lower({lc}) IS DISTINCT FROM lower({rc})"
    return f"{lc} IS DISTINCT FROM {rc}"


def _stream_dicts(con, sql: str):
    """sql 결과를 행당 dict 로 흘려주는 generator factory (접근마다 fresh 커서)."""

    def factory() -> Iterator[dict[str, Any]]:
        cur = con.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        while True:
            rows = cur.fetchmany(_BATCH)
            if not rows:
                break
            for row in rows:
                yield dict(zip(cols, row))

    return factory


def _stream_cells(con, key: list[str], compare_cols: list[str], key_join: str, preds: dict[str, str]):
    """변경 셀을 컬럼별 커서를 이어가며 흘려주는 generator factory.

    컬럼마다 old/new 타입이 달라 한 쿼리로 합치면 공통 타입으로 강제 변환되어 값이
    왜곡될 수 있다. 그래서 컬럼별 쿼리를 순차로 스트리밍해 원래 타입을 보존한다.
    """
    key_sel = ", ".join(f"l.{_q(k)} AS {_q(k)}" for k in key)

    def factory() -> Iterator[CellChange]:
        for c in compare_cols:
            qc = _q(c)
            sql = (
                f"SELECT {key_sel}, l.{qc} AS old_val, r.{qc} AS new_val "
                f"FROM l JOIN r ON {key_join} "
                f"WHERE {preds[c]}"
            )
            cur = con.cursor()
            cur.execute(sql)
            while True:
                rows = cur.fetchmany(_BATCH)
                if not rows:
                    break
                for row in rows:
                    keyvals = {k: row[i] for i, k in enumerate(key)}
                    yield CellChange(key=keyvals, column=c, old=row[-2], new=row[-1])

    return factory


def diff(
    left: "Source",
    right: "Source",
    key: Sequence[str],
    *,
    exclude: Sequence[str] | None = None,
    columns: Sequence[str] | None = None,
    allow_duplicates: bool = False,
    tolerance: float | None = None,
    ignore_case: bool = False,
) -> DiffResult:
    """left 와 right 를 key 기준으로 셀 단위 비교한다.

    반환된 `DiffResult` 는 살아있는 DuckDB 커넥션을 소유한다. 행/셀을 끝까지
    스트리밍하므로 `with diff(...) as result:` 로 쓰거나 `result.close()` 를 호출해
    커넥션을 닫아라.

    Raises:
        DiffError: key 누락/부재, 중복 key(allow_duplicates=False) 등 비교 불가 상황.
    """
    key = list(key)
    if not key:
        # v0: set-diff 폴백은 미구현. 명시적 에러로 끊는다.
        raise DiffError("key 가 필요합니다 (set-diff 폴백은 아직 미지원).")

    exclude_set = set(exclude or [])
    columns_filter = set(columns) if columns else None

    con = duckdb.connect()  # in-memory; 성공 시 DiffResult 가 소유권을 넘겨받는다.
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

        # 셀 비교 술어를 컬럼마다 한 번 만들어 카운트/스트리밍에 공유 (tolerance/ignore_case 반영).
        preds = {
            c: _diff_pred(c, lschema[c], tolerance=tolerance, ignore_case=ignore_case)
            for c in compare_cols
        }

        # --- 카운트(집계로 먼저) — 실제 행/셀은 접근 시 스트리밍한다 ---
        n_removed = con.execute(
            f"SELECT count(*) FROM l ANTI JOIN r ON {key_join}"
        ).fetchone()[0]
        n_added = con.execute(
            f"SELECT count(*) FROM r ANTI JOIN l ON {key_join}"
        ).fetchone()[0]

        changed_rows = 0
        n_changed_cells = 0
        if compare_cols:
            any_diff = " OR ".join(f"({preds[c]})" for c in compare_cols)
            cell_sum = " + ".join(f"({preds[c]})::INT" for c in compare_cols)
            row = con.execute(
                f"SELECT count(*) FILTER (WHERE {any_diff}) AS cr, "
                f"COALESCE(sum({cell_sum}), 0) AS cc "
                f"FROM l JOIN r ON {key_join}"
            ).fetchone()
            changed_rows, n_changed_cells = int(row[0]), int(row[1])

        removed_sql = f"SELECT l.* FROM l ANTI JOIN r ON {key_join}"
        added_sql = f"SELECT r.* FROM r ANTI JOIN l ON {key_join}"

        return DiffResult(
            key=key,
            schema_changes=schema_changes,
            added=_stream_dicts(con, added_sql),
            removed=_stream_dicts(con, removed_sql),
            changed_cells=_stream_cells(con, key, compare_cols, key_join, preds),
            changed_rows=changed_rows,
            counts={
                "added": int(n_added),
                "removed": int(n_removed),
                "changed_cells": n_changed_cells,
            },
            resource=con,
        )
    except BaseException:
        # 결과 반환 전 실패 → 커넥션은 우리가 닫는다 (성공 경로에선 DiffResult 소유).
        con.close()
        raise
