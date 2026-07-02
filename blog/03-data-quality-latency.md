# [이커머스 CDC 파이프라인 #3] dbt 테스트로 데이터 정합성 잡고, CDC 지연을 실측하기 —
그리고 그 과정에서 발견한 진짜 버그 두 개

## 이번 편의 목표

CJ올리브영 공고는 "데이터 정합성/Latency 최소화"를 콕 집어 요구한다. [2편](02-airflow-dbt-duckdb.md)까지
파이프라인이 동작은 했지만, "정말 정합성이 맞는지", "지연이 얼마나 되는지"를 실제로
측정하고 검증하는 장치는 없었다. 이번 편에서 그 장치를 만들었고, 만들자마자 실제
데이터에서 진짜 버그 두 개를 잡아냈다 — 이 글의 핵심은 그 버그들이다.

## Great Expectations 대신 dbt 테스트를 선택한 이유

원래 계획에는 "Great Expectations 등"으로 데이터 품질 도구를 적어뒀다. 하지만 이미
dbt가 파이프라인에 완전히 붙어있는 상태에서 Great Expectations를 새로 얹으면
checkpoint/데이터 컨텍스트/문서화까지 별도 인프라가 필요해진다. dbt의 내장
테스트(`unique`, `not_null`, `accepted_values`, `relationships`)와 커스텀 SQL
테스트(singular test)만으로도 "정합성 검증"이라는 요건을 충분히 보여줄 수 있고,
실무에서도 dbt test는 데이터 품질 검증의 표준적인 선택지 중 하나다. 도구를 늘리는
대신 이미 있는 도구를 더 깊게 쓰는 쪽을 택했다.

추가한 테스트:

```yaml
# dbt/models/staging/schema.yml
stg_orders.user_id      -> relationships: stg_users.user_id
stg_order_items.order_id   -> relationships: stg_orders.order_id
stg_order_items.product_id -> relationships: stg_products.product_id
```

```sql
-- dbt/tests/assert_order_total_matches_items.sql
-- 정합성 체크: orders.total_amount가 실제 order_items 합계와 일치하는지 검증
select o.order_id, o.total_amount as order_total,
       sum(oi.quantity * oi.unit_price) as items_total
from {{ ref('stg_orders') }} o
join {{ ref('stg_order_items') }} oi on oi.order_id = o.order_id
group by o.order_id, o.total_amount
having abs(o.total_amount - sum(oi.quantity * oi.unit_price)) > 0.01
```

또한 `dbt_project.yml`에 `+store_failures: true`를 켜서, 테스트가 실패하면 위반
행을 `main_dq` 스키마에 별도 테이블로 저장하게 했다. 나중에 BI 대시보드에서 "이번
주 DQ 실패 건수 추이"를 보여줄 소재가 된다.

## Latency 모니터링: CDC 이벤트가 얼마나 늦게 도착하는가

Debezium이 캡처하는 이벤트에는 `ts_ms`(변경 발생 시각)가 들어있다. 이 값과 Airflow가
실제로 그 이벤트를 DuckDB에 적재한 시각의 차이를 "지연(latency)"으로 정의하고,
배치마다 테이블별로 평균/최대/p95를 계산해 `pipeline_latency_log`에 남기도록
`ingest_lib.py`를 수정했다.

```python
event_time = datetime.fromtimestamp(event["ts_ms"] / 1000, tz=timezone.utc)
latencies.append((run_ts - event_time).total_seconds())
```

그리고 DAG 마지막에 `data_quality_gate` 태스크를 추가해서, latency가 SLA(DAG
주기 5분의 2배 = 600초)를 넘거나 dbt test가 실패하면 `AirflowException`을 던지도록
했다. Airflow 태스크가 실패하면 UI에 빨갛게 표시되는데, 로컬 프로젝트라 실제
Slack/이메일 알림까지는 안 붙였지만 이 예외를 `on_failure_callback`에 연결하면
바로 알림으로 이어질 수 있는 구조다.

## 버그 1: 같은 밀리초에 커밋된 이벤트의 순서가 뒤바뀐다

`data_quality_gate`를 붙이고 처음 돌렸을 때, `assert_order_total_matches_items`
테스트가 실제로 실패했다. 주문 몇 건의 `total_amount`가 0으로 나온 것이다.

원인을 추적해보니 `generator/db.py`에서 `conn.autocommit = True`로 설정돼 있어서,
주문 생성 과정의 각 SQL문(`orders` insert → `order_items` insert들 → `orders`
total_amount update)이 **각각 별도 트랜잭션**으로 즉시 커밋되고 있었다. 문제는
이 문장들이 너무 빨리 실행돼서, insert와 그 직후의 update가 **같은 밀리초**에
커밋되는 경우가 실제로 있었다는 것이다.

```
order_id  total_amount  __op  __ts_ms
48        0.0           c     1782979007761
48        33962.3       u     1782979007761   <- insert와 같은 밀리초!
```

staging 뷰는 `QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY __ts_ms
DESC) = 1`로 "최신 상태"를 골랐는데, `__ts_ms`가 동률이면 어느 행이 1등으로
뽑힐지 SQL 엔진이 보장해주지 않는다. 실제로 DuckDB가 종종 update가 아니라 insert
쪽(즉 초기값 0)을 골라버렸다.

**해결**: DuckDB `SEQUENCE`로 "적재 순서"를 보장하는 `__seq` 컬럼을 새로
도입했다. GCS에서 파일을 읽는 순서(파일명 정렬)와 파일 내 라인 순서(=Kafka 컨슈머가
실제로 소비한 순서)를 그대로 반영하는 단조 증가 값이라, 밀리초 단위로는 구분이 안
되는 이벤트도 정확한 도착 순서로 구분할 수 있다. staging 뷰의 정렬 기준을
`ORDER BY __ts_ms DESC, __seq DESC`로 바꿔서 해결했다.

```python
con.execute("CREATE SEQUENCE IF NOT EXISTS event_seq START 1")
...
row["__seq"] = con.execute("SELECT nextval('event_seq')").fetchone()[0]
```

이 버그가 흥미로운 이유는, "타임스탬프 정밀도가 이벤트 발생 빈도보다 낮으면
순서 보장이 깨질 수 있다"는, CDC 시스템을 다뤄보지 않으면 잘 떠오르지 않는
함정이기 때문이다. 실제 운영 환경에서 소스 DB가 초당 여러 건의 트랜잭션을
처리한다면 이런 동률은 훨씬 자주 발생할 수 있다.

## 버그 2: 자식 레코드가 부모보다 먼저 도착하는 배치 경계 문제

`__seq` 버그를 고치고 나니, 이번엔 `relationships` 테스트가 실패했다 —
`order_items`의 `order_id` 6건이 `stg_orders`에 존재하지 않는다는 것이었다.

`orders`와 `order_items`는 Debezium에서 서로 다른 Kafka 토픽으로 나뉘고,
`lake-writer`는 토픽마다 독립적인 타이밍으로 버퍼를 GCS 파일에 flush한다. 그러다
보니 특정 배치 시점에 `order_items` 스냅샷 파일은 이미 GCS에 올라갔는데,
같은 주문의 `orders` 스냅샷 파일은 아직 flush 전이라 이번 배치에 포함되지 못하는
상황이 생길 수 있다. 이 경우 그 배치 시점의 `stg_order_items`에는 있지만
`stg_orders`에는 없는 "고아 레코드"가 일시적으로 나타난다.

다음 배치를 돌려보니 이 6건은 저절로 사라졌다 — 다음 주기에 `orders` 스냅샷이
따라잡히면서 정상화된 것이다. 이건 코드 버그가 아니라, **여러 개의 독립적인 CDC
스트림을 배치로 묶을 때 필연적으로 생기는 일시적 정합성 지연**이다. 고쳐야 할
"버그"라기보다는, 실제 운영이라면 "이런 위반이 몇 분 이상 지속되면 알림, 한 배치
안에서 해소되면 정상"으로 판단 기준을 다르게 잡아야 한다는 걸 보여주는 사례다.
지금 `data_quality_gate`는 매 배치마다 판단하기 때문에 이런 순간적 불일치도
일단 실패로 잡아내는데, 이건 의도적으로 "보수적으로 잡고, 지속 여부는 이후
배치 로그로 추적한다"는 방향으로 남겨뒀다.

## 검증

버그 두 개를 고친 뒤 웨어하우스를 초기화하고(스키마 변경이라 `__seq` 컬럼을 새로
추가해야 해서 GCS raw 데이터로부터 전체 재적재) DAG를 연속으로 여러 번 트리거해서
안정적으로 success가 나오는 걸 확인했다.

```
[dq] latency_sla: PASS - 모든 테이블 SLA 이내 (최대 latency 78s)
[dq] dbt_test: PASS - dbt test 전부 통과
```

## 다음 편

다음 편에서는 이 마트 데이터를 Looker Studio로 시각화해서 실제 BI 대시보드를
만드는 과정을 다룬다.
