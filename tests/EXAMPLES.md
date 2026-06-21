# lake-sift 예제 모음

용도별 실무 시나리오 모음입니다. **아래 출력 블록은 전부 실제 CLI 실행 결과**이며,
손으로 적은 게 아닙니다 — 보이는 그대로 나옵니다.

대부분의 예제는 작은 `orders` 테이블을 어떤 변경 *전(before)* vs *후(after)* 로 비교합니다:

| order_id | status  | amount | updated_at |
|---------:|---------|-------:|------------|
| (before) 1 | paid    | 10.00  | t1 |
| (before) 2 | paid    | 20.00  | t1 |
| (before) 3 | pending | 30.00  | t1 |
| (before) 4 | paid    | 40.00  | t1 |

변환/백필 이후: order 1 은 사라지고, order 5 가 새로 생기고, order 3 은
`pending → shipped`, order 4 의 amount 가 `40.00 → 40.005` 로 미세하게 흔들렸고,
모든 행의 `updated_at` 이 새로 쓰였습니다.

모든 예제의 종료 코드: **`0`** = 동일, **`1`** = 차이 있음, **`2`** = 에러.

---

## 1. CI 게이트 — 변환이 결과 데이터를 바꾸면 PR 차단

대표 유스케이스: CI 에서 돌려서, 데이터가 예상치 못하게 바뀌면 0 이 아닌 종료 코드로
빌드를 실패시킨다.

```bash
lake-sift before.parquet after.parquet -k order_id || echo "data changed — review!"
```

```text
+1 added  -1 removed  ~3 changed rows (5 cells)
  top changed columns: updated_at (3), status (1), amount (1)
- order_id=1, status='paid', amount=10.0, updated_at='t1'
+ order_id=5, status='pending', amount=50.0, updated_at='t2'
~ [order_id=3] status: 'pending' → 'shipped'
~ [order_id=4] amount: 40.0 → 40.005
~ [order_id=2] updated_at: 't1' → 't2'
~ [order_id=3] updated_at: 't1' → 't2'
~ [order_id=4] updated_at: 't1' → 't2'
exit: 1
```

`- ` = 왼쪽에만 있는 행(삭제), `+ ` = 오른쪽에만 있는 행(추가),
`~ [key] col: old → new` = 변경된 셀. `top changed columns` 줄은 변경이 어느 컬럼에
몰려 있는지를 보여준다.

## 2. 빠른 확인 — 요약만

헤드라인 카운트만 보고 싶을 때(예: CI 로그 한 줄). 행별 상세는 생략된다.

```bash
lake-sift before.parquet after.parquet -k order_id --summary
```

```text
+1 added  -1 removed  ~3 changed rows (5 cells)
  top changed columns: updated_at (3), status (1), amount (1)
exit: 1
```

## 3. 변동성 큰 컬럼 무시 — `--exclude`

`updated_at` 은 실행할 때마다 바뀌고 실제 데이터 변경이 아니다. 제외하면 진짜 중요한
변경만 보인다.

```bash
lake-sift before.parquet after.parquet -k order_id -x updated_at
```

```text
+1 added  -1 removed  ~2 changed rows (2 cells)
  top changed columns: status (1), amount (1)
- order_id=1, status='paid', amount=10.0
+ order_id=5, status='pending', amount=50.0
~ [order_id=3] status: 'pending' → 'shipped'
~ [order_id=4] amount: 40.0 → 40.005
exit: 1
```

제외한 `updated_at` 은 추가/삭제 행에서도 빠진다 —
[컬럼 범위 지정](#4-특정-컬럼만-비교--columns) 참고.

## 4. 특정 컬럼만 비교 — `--columns`

"어떤 주문의 `status` 가 바뀌었나?" 그 컬럼만 비교한다.

```bash
lake-sift before.parquet after.parquet -k order_id -c status
```

```text
+1 added  -1 removed  ~1 changed rows (1 cells)
  top changed columns: status (1)
- order_id=1, status='paid'
+ order_id=5, status='pending'
~ [order_id=3] status: 'pending' → 'shipped'
exit: 1
```

## 5. 부동소수점 허용 오차 — `--tolerance`

`amount` 가 반올림 때문에 `0.005` 흔들렸을 뿐 실제 변경은 아니다. 허용 오차 `0.01` 은
그 델타 이내 값을 동일로 본다(수치 컬럼만 — 문자열은 영향 없음).

```bash
lake-sift before.parquet after.parquet -k order_id -t 0.01
```

```text
+1 added  -1 removed  ~3 changed rows (4 cells)
  top changed columns: updated_at (3), status (1)
- order_id=1, status='paid', amount=10.0, updated_at='t1'
+ order_id=5, status='pending', amount=50.0, updated_at='t2'
~ [order_id=3] status: 'pending' → 'shipped'
~ [order_id=2] updated_at: 't1' → 't2'
~ [order_id=3] updated_at: 't1' → 't2'
~ [order_id=4] updated_at: 't1' → 't2'
exit: 1
```

`amount` 셀(`40.0 → 40.005`)이 빠지면서 변경 셀이 5개에서 4개가 됐다.

## 6. 대소문자 무시 — `--ignore-case`

두 `name` 컬럼이 대소문자만 다르다(`Alice`/`alice`, `Bob`/`BOB`).

```bash
lake-sift c1.parquet c2.parquet -k id -i
```

```text
= no differences
exit: 0
```

`-i` 가 없으면 대소문자도 변경으로 잡힌다:

```text
+0 added  -0 removed  ~2 changed rows (2 cells)
  top changed columns: name (2)
~ [id=1] name: 'Alice' → 'alice'
~ [id=2] name: 'Bob' → 'BOB'
exit: 1
```

## 7. 기계용 JSON — `--json`

대시보드, 봇, 또는 `jq` 파이프용. compact JSON 한 덩어리이고, 행/셀은 스트리밍되므로
큰 diff 도 견딘다.

```bash
lake-sift before.parquet after.parquet -k order_id --json
```

```json
{"key":["order_id"],"summary":{"added":1,"removed":1,"changed":3,"changed_cells":5,"schema_changes":0},"schema_changes":[],"changed_by_column":[{"column":"updated_at","count":3},{"column":"status","count":1},{"column":"amount","count":1}],"added":[{"order_id":5,"status":"pending","amount":50.0,"updated_at":"t2"}],"removed":[{"order_id":1,"status":"paid","amount":10.0,"updated_at":"t1"}],"changed_cells":[{"key":{"order_id":3},"column":"status","old":"pending","new":"shipped"},{"key":{"order_id":4},"column":"amount","old":40.0,"new":40.005},{"key":{"order_id":2},"column":"updated_at","old":"t1","new":"t2"},{"key":{"order_id":3},"column":"updated_at","old":"t1","new":"t2"},{"key":{"order_id":4},"column":"updated_at","old":"t1","new":"t2"}]}
exit: 1
```

## 8. 스키마 변경 — 컬럼 이름 변경 / 타입 변경

컬럼이 추가·삭제되거나 타입이 바뀌면 맨 위에 보고된다(차이이므로 종료 코드 `1`).

```bash
lake-sift s1.parquet s2.parquet -k id     # 왼쪽엔 컬럼 v, 오른쪽엔 컬럼 w
```

```text
- column v (VARCHAR)
+ column w (VARCHAR)
+0 added  -0 removed  ~0 changed rows (0 cells)
exit: 1
```

## 9. 복합 키 — 팩트 / 라인아이템 테이블

행을 두 개 이상의 컬럼으로 식별한다. 쉼표로 구분해 넘긴다.

```bash
lake-sift k1.parquet k2.parquet -k order_id,line_no
```

```text
+0 added  -0 removed  ~1 changed rows (1 cells)
  top changed columns: qty (1)
~ [order_id=1, line_no=2] qty: 3 → 9
exit: 1
```

## 10. 동일한 입력

```bash
lake-sift i1.parquet i2.parquet -k id
```

```text
= no differences
exit: 0
```

## 11. 중복 키 — 에러, 그리고 `--allow-duplicates` 주의점

기본적으로 키가 유일하지 않으면 에러(종료 코드 `2`)다 — 셀 단위 diff 는 1:1 행 매칭이
필요하기 때문이다:

```bash
lake-sift d1.parquet d2.parquet -k id
```

```text
error: left has duplicate keys. Use --allow-duplicates to bypass.
exit: 2
```

`--allow-duplicates` 는 검사를 우회하지만 **주의**: 중복 키가 있으면 매칭 조인이
교차 곱(cross product)이 되어 셀 diff 가 노이즈/오해 소지가 생긴다. 가능하면 우회보다
키를 고치는 편이 낫다.

```bash
lake-sift d1.parquet d2.parquet -k id --allow-duplicates   # id=1 이 양쪽에 두 번씩
```

```text
+0 added  -0 removed  ~2 changed rows (2 cells)
  top changed columns: v (2)
~ [id=1] v: 'a' → 'b'
~ [id=1] v: 'b' → 'a'
exit: 1
```

---

## 12. Delta Lake — 버전 간 타임트래블

같은 Delta 테이블의 두 버전 사이에 무엇이 바뀌었는지 감사한다.
`delta:<path-or-uri>[@<version>]` 피연산자를 쓴다(`pip install "lake-sift[delta]"` 필요).

```bash
lake-sift delta:/data/sales@0 delta:/data/sales@1 -k order_id
```

```text
+0 added  -0 removed  ~1 changed rows (1 cells)
  top changed columns: status (1)
~ [order_id=2] status: 'pending' → 'shipped'
exit: 1
```

## 13. 포맷 혼합 — Parquet 추출본을 라이브 레이크하우스 테이블과 검증

피연산자는 포맷을 자유롭게 섞을 수 있다. 여기선 Parquet 추출본을 라이브 Delta 테이블
(최신 버전)과 대조한다 — 일치하므로 추출본이 정확하다는 뜻.

```bash
lake-sift export.parquet delta:/data/sales -k order_id
```

```text
= no differences
exit: 0
```

## 14. Apache Iceberg — 두 스냅샷 비교

`iceberg:<catalog>/<namespace>.<table>[@<snapshot_id>]` 피연산자로 동일하게 한다
(`pip install "lake-sift[iceberg]"` 필요). 카탈로그 접속 정보는 PyIceberg 표준 설정
(`~/.pyiceberg.yaml` / `PYICEBERG_*` 환경변수)에서 읽으며, lake-sift 는 카탈로그를
이름으로만 참조한다.

```bash
# 같은 테이블의 두 스냅샷 사이 변경 감사
lake-sift "iceberg:prod/sales.orders@1001" "iceberg:prod/sales.orders@1042" -k order_id

# 또는 Parquet 추출본을 라이브 테이블과 검증
lake-sift export.parquet "iceberg:prod/sales.orders" -k order_id
```

`@` 뒤가 정수면 snapshot id, 그 외는 **브랜치/태그 이름**으로 해석된다. 출력 형식은
위 Parquet 예제들과 동일하다(같은 diff 코어).

## 14-1. Write-Audit-Publish (WAP) — Iceberg 브랜치 감사 게이트

레이크하우스에서 변경을 **스테이징 브랜치에 쓰고(Write) → main 과 diff 해서 검증하고
(Audit) → 의도한 변경만 있으면 머지(Publish)** 하는 패턴. lake-sift 가 그 Audit 단계다.
`@main` / `@staging` 처럼 브랜치 이름을 그대로 쓴다.

```bash
# 머지 전에 staging 브랜치를 main 과 비교 — 차이가 있으면(exit 1) publish 보류
lake-sift "iceberg:prod/sales.orders@main" "iceberg:prod/sales.orders@staging" -k order_id \
  || echo "staging 이 main 과 다름 — publish 전에 검토"
```

예: staging 브랜치에 주문 하나(`order_id=4`)만 추가됐다면 —

```text
+1 added  -0 removed  ~0 changed rows (0 cells)
+ order_id=4, status='new'
exit: 1
```

종료 코드(0/1)로 CI·오케스트레이터에서 publish 를 막는 게이트로 쓴다. 실제 머지
(fast-forward)는 카탈로그 관리 영역이라 lake-sift 밖이다. (Delta 는 네이티브 브랜치가
없어 [버전 타임트래블](#12-delta-lake--버전-간-타임트래블)로 같은 검증을 한다.)

---

## 15. Python API — 테스트, 노트북, 파이프라인 안에서

CLI 는 얇은 래퍼이고 실제 표면은 라이브러리다. 결과는 살아있는 커넥션을 소유하므로
(행/셀은 스트리밍됨) 컨텍스트 매니저로 쓴다.

```python
from lakesift import diff, ParquetSource

with diff(
    ParquetSource("a.parquet"),
    ParquetSource("b.parquet"),
    key=["id"],
    exclude=["updated_at"],
) as r:
    print("summary:", r.summary())
    print("added:", list(r.added))
    print("removed:", list(r.removed))
    print("changed_cells:", [(c.key, c.column, c.old, c.new) for c in r.changed_cells])
```

```text
summary: {'added': 1, 'removed': 1, 'changed': 1, 'changed_cells': 1, 'schema_changes': 0}
added: [{'id': 4, 'v': 'd'}]
removed: [{'id': 1, 'v': 'a'}]
changed_cells: [({'id': 3}, 'v', 'c', 'C')]
```

데이터 테스트에서 흔한 단언:

```python
with diff(ParquetSource("expected.parquet"), ParquetSource("actual.parquet"), key=["id"]) as r:
    assert r.is_empty(), r.summary()
```

`added`/`removed`/`changed_cells` 는 **이터레이터**(접근마다 새로 생성)다 — 개수는
`summary()`, 전체 목록은 `list(...)` 를 쓴다. Iceberg/Delta 도 `IcebergSource` /
`DeltaSource` 로 동일하게 동작한다.

### 컬럼 범위 지정에 대한 메모

`added`/`removed` 는 보통 전체 행을 보여준다. `--exclude`/`--columns`(CLI) 또는
`exclude=`/`columns=`(API)로 diff 를 좁히면, lake-sift 는 각 소스에서 key + 비교 대상
컬럼만 읽으므로(스캔에 pushdown) 그 행들도 해당 컬럼만 보인다. 스키마 변경은 여전히
전체 스키마 기준으로 감지된다.
