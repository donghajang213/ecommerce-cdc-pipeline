# [이커머스 CDC 파이프라인 #2] Airflow + dbt + DuckDB로 배치 마트 만들기

## 이번 편의 목표

[1편](01-cdc-kafka-datalake.md)에서 Postgres의 CDC 이벤트가 `fake-gcs-server`
버킷에 `raw/<table>/dt=YYYY-MM-DD/*.jsonl`로 쌓이는 것까지 만들었다. 이번에는 이
raw 데이터를 Airflow가 주기적으로 읽어서 웨어하우스에 적재하고, dbt로
staging → mart 레이어를 빌드하는 배치 파이프라인을 구축한다. 넥슨/커머스 BI 공고가
요구하는 "Airflow/dbt 기반 데이터마트 모델링" 요건을 직접 겨냥한 단계다.

## 왜 BigQuery 대신 DuckDB인가

목표는 최종적으로 실제 GCP(BigQuery)로 전환하는 것이지만, 로컬 개발 단계에서는
DuckDB를 썼다. 컬럼형 저장 구조와 분석 쿼리 성능 특성이 BigQuery와 가장 비슷하면서도
서버가 필요 없고, `dbt-duckdb` 어댑터가 있어서 dbt 모델을 그대로 짤 수 있다. 나중에
실제 GCP로 전환할 때는 어댑터를 `dbt-bigquery`로 교체하기만 하면 되도록 설계했다 —
로컬에서 검증한 SQL 로직을 거의 그대로 재사용할 수 있는 구조다.

Airflow는 Celery/Redis 없이 LocalExecutor + 전용 메타데이터 Postgres(`airflow-db`)로
구성했다. 학습/시연 목적의 로컬 프로젝트에 Celery 클러스터는 과했고, 실무에서도 소규모
파이프라인은 LocalExecutor로 충분한 경우가 많다.

## 아키텍처

```
fake-gcs-server(raw/<table>/dt=.../*.jsonl)
   → Airflow DAG(shop_pipeline, */5min)
      1) ingest_raw_to_duckdb: GCS의 새 파일만 골라 읽어서 DuckDB raw 테이블에 적재
         (Debezium 이벤트의 op/ts_ms를 __op/__ts_ms 메타컬럼으로 남겨 append-only 로그로 저장)
      2) dbt_build: dbt run + dbt test
         staging(뷰): PK별 최신 상태만 남김 (QUALIFY ROW_NUMBER() ... = 1)
         marts(테이블): fct_daily_sales, fct_product_sales, mart_user_purchase_summary
```

핵심 설계 포인트는 raw 테이블을 **append-only 로그**로 유지한다는 것이다. CDC
이벤트는 같은 기본키에 대해 insert/update가 여러 번 들어올 수 있는데, 이걸 raw
단계에서 덮어쓰지 않고 이벤트 그대로 쌓은 다음, staging 뷰에서
`QUALIFY ROW_NUMBER() OVER (PARTITION BY pk ORDER BY __ts_ms DESC) = 1`로 "현재
가장 최신 상태"만 골라내는 방식을 썼다. 이렇게 하면 나중에 이력 조회나 SCD 스타일
분석이 필요해져도 raw 데이터를 다시 수집할 필요 없이 staging 로직만 바꾸면 된다.

원래 계획에는 "사용자 퍼널 마트"(클릭스트림 기반)도 있었지만, page_view/cart_event
등 클릭스트림 이벤트 스트림을 아직 만들지 않아서 이번 단계에서는
`mart_user_purchase_summary`(재구매 여부/LTV 요약)로 대체했다. 무리해서 있지도 않은
데이터를 억지로 채우기보다, 지금 가진 데이터로 의미 있는 마트를 만드는 쪽을 택했다.

## 트러블슈팅

**DuckDB 웨어하우스 파일 "Permission denied"**
`airflow-init` 컨테이너에서 기본 entrypoint를 `/bin/bash`로 덮어썼더니, Airflow
공식 이미지가 원래 처리해주던 볼륨 권한 설정을 우회하게 됐다. Docker 이름 있는
볼륨은 "처음 마운트될 때 이미지 안의 동일 경로 소유권을 그대로 복사"하는 특성이
있어서, `Dockerfile`에 `RUN mkdir -p /opt/airflow/warehouse`를 미리 추가해
airflow 유저 소유로 만들어두는 방식으로 해결했다. entrypoint를 함부로 덮어쓰면
이미지가 암묵적으로 해주던 일까지 같이 사라진다는 걸 배웠다.

**`dbt-duckdb`와 `duckdb` 버전 충돌**
`requirements.txt`에 두 패키지 버전을 각각 고정했더니 pip 의존성 충돌이 났다.
`dbt-duckdb`가 내부적으로 요구하는 `duckdb` 버전 범위가 있어서, `duckdb` 버전
고정을 빼고 pip가 알아서 호환 버전을 고르게 하니 해결됐다. 어댑터 패키지를 쓸 때는
기반 라이브러리 버전을 직접 고정하기 전에 어댑터가 요구하는 범위부터 확인하는 게
낫다.

**DuckDB `Conversion Error: DOUBLE -> TIMESTAMP`**
Debezium이 타임스탬프 컬럼을 마이크로초 단위 정수로 보내는데, 이걸 초 단위 float로만
변환하고 DuckDB TIMESTAMP 컬럼에 그대로 넣으려다 실패했다.
`datetime.fromtimestamp(..., tz=timezone.utc)`로 실제 datetime 객체를 만들어서
넣는 방식으로 고쳤다. "숫자를 시간처럼 보이게 변환"과 "실제 시간 타입으로 변환"은
다르다는, 기본이지만 놓치기 쉬운 지점이었다.

**dbt 스키마 이름이 예상과 다름**
`dbt_project.yml`에 `+schema: marts`로 설정했는데 실제 생성된 스키마는
`main_marts`였다. dbt는 기본적으로 `<디폴트 스키마>_<커스텀 스키마>` 형태로
접두사를 붙이는 게 기본 동작이다(커스텀 `generate_schema_name` 매크로를 안 쓰는 한).
이 규칙을 모르고 쿼리하면 "테이블이 없다"는 에러에 한참 헤맬 수 있다.

## 검증

Airflow DAG(`shop_pipeline`, 5분 주기)를 수동 트리거해서 end-to-end로 확인했다.

- `ingest_raw_to_duckdb`: GCS raw 파일(약 2,800개 CDC 이벤트)을 DuckDB raw
  테이블에 적재 성공
- `dbt_build`: staging 5개 + mart 3개 모델 전부 빌드 + dbt test 통과
- DuckDB에서 직접 쿼리해서 마트 데이터 정합성 확인: 일별 매출(468건 주문, 약
  1.5억원), 상품별 매출 Top5, 재구매 고객 LTV Top5 모두 정상 집계됨

## 다음 편

다음 편에서는 CJ올리브영 공고가 콕 집어 요구한 "데이터 정합성/Latency 최소화"를
직접 겨냥해서, dbt 테스트 기반 정합성 검증과 CDC 이벤트 지연(latency) 모니터링을
추가한 이야기를 다룬다. 그 과정에서 실제로 발견한 CDC 파이프라인의 재미있는 버그
두 가지도 소개한다.
