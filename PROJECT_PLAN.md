# 데이터 엔지니어링 포트폴리오 프로젝트

## 목표
데이터 엔지니어 이직/지원을 위한 포트폴리오 프로젝트. 아래 채용공고 3건의 요구사항을 하나의 엔드투엔드 파이프라인으로 커버하는 것이 목표.

최종 산출물:
- **GitHub**: 전체 코드 + 아키텍처 문서
- **Tistory 블로그**: 개발 과정/설계 의사결정/트러블슈팅 회고를 시리즈로 포스팅

## 타겟 채용공고 요약

| 회사 | 연차 | 핵심 요구사항 |
|---|---|---|
| CJ올리브영 | 5년+ | Kafka, CDC(OGG/Debezium), 실시간 스트리밍 파이프라인, RestAPI Bulk 수집, 이기종 DB 동기화, 데이터 정합성/Latency 최소화, Spark/Airflow, K8s/CI/CD, AWS/GCP |
| 넥슨 (플랫폼본부) | 3년+ | SQL/Python 데이터 가공, 대규모 배치 파이프라인, DW/DM 모델링, Airflow/DBT, 클라우드(GCP/AWS/Snowflake 등) |
| 커머스 BI/데이터마트 | 3년+ | SQL/Python/Airflow, BigQuery/Redshift, 데이터마트 모델링, Tableau/Looker Studio 대시보드 |

**공통 요구사항**: Python/SQL, Airflow, 클라우드, 데이터마트 설계
**CJ만의 특화 요구사항**: Kafka + CDC + 정합성 검증
**BI 공고 특화**: 시각화 대시보드

## 아키텍처 (초안)

```
[가짜 소스 DB (Postgres/MySQL): users, products, orders, order_items, inventory]
        │  (CDC)
        ▼
   Debezium → Kafka (실시간 스트리밍)
        │
        ├── (별도) Kafka Producer: page_view, cart_event, search_event (클릭스트림/이벤트)
        ▼
   GCS (Raw/Bronze Data Lake)
        │
        ▼
   Airflow (배치 오케스트레이션, 스케줄/재처리)
        │
        ▼
   dbt or Python 변환 → BigQuery (Silver/Gold, 데이터마트)
        │
        ▼
   Looker Studio / Tableau (BI 대시보드)

   + 데이터 정합성 검증 (Great Expectations 등)
   + 모니터링/알림 (지연(latency) 추적)
```

## 도메인 & 데이터 선택
**이커머스 주문/이벤트 데이터**로 결정.

이유:
- CDC 데모에 최적 (orders/inventory 변경 → Debezium 캡처가 자연스러움) → CJ올리브영 요건 직결
- 클릭스트림/장바구니 이벤트를 Kafka producer로 별도 발행 → 정형+비정형(반정형) 통합 스토리 완성
- 주문 데이터는 매출 마트/퍼널 마트/재구매율 지표로 자연스럽게 귀결 → 넥슨/BI 공고의 DW·대시보드 요건 커버

**데이터 소스**: 실제 공개 데이터셋(Kaggle "Online Retail" 또는 "Brazilian E-Commerce - Olist")을 시드로 사용하고, Python으로 실시간 이벤트를 합성 생성하는 스크립트를 추가하는 방식.

### 테이블/스트림 구성 (안)
- **CDC 대상 (RDB)**: `users`, `products`, `orders`, `order_items`, `inventory`
- **스트리밍 이벤트 (Kafka 직접 발행)**: `page_view`, `cart_event`, `search_event`
- **최종 데이터마트**: 일별 매출 마트, 상품별 판매 마트, 사용자 퍼널 마트

## 로드맵 / 우선순위
1. MVP: 가짜 소스 → Debezium/Kafka → GCS → Airflow → BigQuery 파이프라인 완성
2. dbt로 staging → mart 레이어 모델링 (SCD 등 이력관리 포함)
3. 데이터 품질 체크(Great Expectations) + 파이프라인 지연 모니터링 추가 (CJ 요건 직접 겨냥)
4. Looker Studio 대시보드 구축 (BI 요건)
5. (여유되면) Docker/K8s 배포 + CI/CD 파이프라인 (우대사항 커버)

## 실행 환경 결정사항 (2026-07-01)
- **GCP**: 로컬 에뮬레이션 우선 (GCS → `fake-gcs-server`, BigQuery → 로컬 Postgres/DuckDB), 파이프라인 완성 후 실제 GCP로 전환.
- **로컬 인프라**: Docker Desktop 설치 확인됨. Kafka/Debezium/Airflow/Postgres 전부 Docker Compose로 구성.
- **시작 범위**: 1단계부터 순서대로 진행 (CDC + Kafka + 데이터레이크 → Airflow/dbt/BigQuery → BI → 배포/CI-CD).
- **시드 데이터**: 외부 Kaggle 데이터셋 다운로드 대신 Faker로 자체 합성 데이터 생성 (재현성/자체완결성 확보 — 리뷰어가 별도 계정 없이 `docker compose up`만으로 재현 가능하도록).

## 1단계 상세 설계 (진행 중)
```
Postgres(source: users/products/orders/order_items/inventory)
   → simulate.py (지속적 주문 생성/상태변경/재고차감 → CDC 이벤트 유발)
   → Debezium (Kafka Connect) → Kafka topics (topic per table)
   → lake-writer consumer → fake-gcs-server bucket (raw/<table>/dt=YYYY-MM-DD/*.json)
```
레포 구조:
```
docker-compose.yml
source-db/init/*.sql       (스키마)
seed/seed.py                (Faker 초기 데이터)
simulate/simulate.py         (지속적 CDC 이벤트 생성기)
debezium/register-connector.json
lake-writer/consumer.py, Dockerfile, requirements.txt
```

## 1단계 검증 결과 (완료)
`docker compose up`으로 아래 파이프라인이 end-to-end로 동작함을 확인함:

```
Postgres(source) --Debezium CDC--> Kafka topics(shop.public.*)
   --lake-writer consumer--> fake-gcs-server 버킷(dl-raw)
   raw/<table>/dt=YYYY-MM-DD/*.jsonl
```

검증 방법: `kafka-console-consumer`로 토픽에 실제 CDC 이벤트(op=r 스냅샷, op=u 업데이트 등) 확인 → lake-writer 로그에서 `Wrote N records to gs://...` 확인 → `curl http://localhost:4443/storage/v1/b/dl-raw/o` 로 실제 오브젝트 생성 확인.

### 트러블슈팅 기록 (블로그 소재)
- **Docker Desktop "Virtualization support not detected"**: Windows의 Hyper-V/Virtual Machine Platform/WSL 기능이 꺼져 있어서 발생. `Enable-WindowsOptionalFeature`로 활성화 후 재시작하여 해결. (하드웨어/BIOS 가상화는 이미 켜져 있었음 — `systeminfo`의 Hyper-V Requirements 로 확인)
- **`debezium/connect:2.7` 이미지 태그 없음**: Docker Hub에는 `2.7.3.Final`처럼 정확한 패치 버전 태그만 존재. `2.7.3.Final`로 수정.
- **lake-writer가 `UNKNOWN_TOPIC_OR_PART`로 죽음**: 정규식 패턴 구독(`^shop\..*`) 시 아직 매칭되는 토픽이 없으면 발생하는 일시적 에러인데 무조건 raise 하도록 짜서 컨테이너가 크래시함. 해당 에러 코드는 무시하고 계속 poll 하도록 수정.
- **컨테이너 로그가 안 보임**: Python이 파이프로 리다이렉트될 때 stdout을 블록 버퍼링해서 `docker logs`에 아무것도 안 찍힘. `Dockerfile`에 `ENV PYTHONUNBUFFERED=1` 추가로 해결.

## 2단계 실행 환경 결정사항 (2026-07-01)
- **웨어하우스**: DuckDB (BigQuery의 분석/컬럼형 특성과 가장 비슷, 서버 불필요, `dbt-duckdb` 어댑터 사용 — 나중에 실제 GCP 전환 시 `dbt-bigquery`로 어댑터만 교체하면 됨)
- **Airflow 구성**: LocalExecutor + 전용 메타데이터 Postgres(`airflow-db`) + webserver + scheduler. Celery/Redis 없이 실무에 가까운 최소 구성.

## 2단계 상세 설계

```
fake-gcs-server(raw/<table>/dt=.../*.jsonl)
   → Airflow DAG(shop_pipeline, */5min)
      1) ingest_raw_to_duckdb: GCS의 새 파일만 골라 읽어서 DuckDB raw 테이블에 적재
         (Debezium 이벤트의 op/ts_ms를 __op/__ts_ms 메타컬럼으로 남겨 append-only 로그로 저장)
      2) dbt_build: dbt run + dbt test
         staging(뷰): PK별 최신 상태만 남김 (QUALIFY ROW_NUMBER() ... = 1)
         marts(테이블): fct_daily_sales, fct_product_sales, mart_user_purchase_summary
```

레포 구조 추가분:
```
airflow/Dockerfile
airflow/dags/ingest_lib.py      (GCS -> DuckDB raw 적재 로직)
airflow/dags/shop_pipeline.py   (DAG 정의: ingest -> dbt run/test)
dbt/dbt_project.yml, profiles.yml
dbt/models/staging/*.sql, sources.yml, schema.yml
dbt/models/marts/*.sql, schema.yml
```

**참고**: 원래 계획했던 "사용자 퍼널 마트"는 클릭스트림 이벤트(page_view/cart_event/search_event) 스트림이 아직 없어서 이번 단계에서는 `mart_user_purchase_summary`(재구매 여부/LTV 요약)로 대체함. 클릭스트림 프로듀서는 여유 있으면 추가할 후속 과제로 남겨둠.

## 2단계 검증 결과 (완료)
Airflow DAG(`shop_pipeline`, 5분 주기)를 수동 트리거해서 end-to-end 확인:
- `ingest_raw_to_duckdb`: GCS raw 파일(약 2,800개 CDC 이벤트)을 DuckDB raw 테이블에 적재 (성공)
- `dbt_build`: staging 5개 + mart 3개 모델 전부 빌드 + dbt test 통과 (성공)
- DuckDB에서 직접 쿼리해서 마트 데이터 정합성 확인: 일별 매출(468건 주문, 약 1.5억원), 상품별 매출 Top5, 재구매 고객 LTV Top5 모두 정상 집계됨.

### 트러블슈팅 기록 (추가)
- **DuckDB 웨어하우스 파일 "Permission denied"**: `airflow-init`에서 기본 entrypoint를 `/bin/bash`로 덮어써서 Airflow 이미지가 원래 해주던 볼륨 권한 처리를 우회하게 됨. 이름 있는 도커 볼륨은 "처음 마운트될 때 이미지 안의 동일 경로 소유권을 그대로 복사"하므로, Dockerfile에 `RUN mkdir -p /opt/airflow/warehouse`를 추가해 airflow 유저 소유로 미리 만들어두는 방식으로 해결.
- **`dbt-duckdb`와 `duckdb` 버전 충돌**: 버전을 둘 다 직접 고정하면 pip 의존성 충돌 발생. `dbt-duckdb`가 요구하는 `duckdb` 버전을 pip가 알아서 고르도록 duckdb 버전 고정을 제거.
- **DuckDB `Conversion Error: DOUBLE -> TIMESTAMP`**: Debezium의 마이크로초 타임스탬프를 초 단위 float로만 변환하고 DuckDB에 그대로 넣으려다 실패. `datetime.fromtimestamp(..., tz=utc)`로 실제 datetime 객체를 만들어 넣도록 수정.
- **dbt 스키마 이름이 예상과 다름**: `+schema: marts`로 설정해도 실제 생성되는 스키마는 `main_marts`(디폴트 스키마 + 커스텀 스키마 접두 규칙, dbt 기본 동작). 쿼리할 때 이 규칙을 알아야 헷갈리지 않음.

## 진행 상황 로그
- 2026-07-01: 기획 완료. 타겟 공고 3건 분석, 아키텍처 초안 확정, 도메인 데이터(이커머스) 결정. 다음 단계는 데이터셋 확정 및 스키마 상세 설계.
- 2026-07-01: 실행 환경 결정(로컬 에뮬레이션, Docker Compose, Faker 시드 데이터). 1단계(CDC+Kafka+데이터레이크) 스캐폴딩 착수.
- 2026-07-01: 1단계 완료 — Postgres/Debezium/Kafka/lake-writer/fake-gcs-server 전체 스택이 docker compose로 기동되고, CDC 이벤트가 실제로 데이터레이크에 JSONL로 적재되는 것까지 검증함. 다음 단계는 Airflow + dbt + BigQuery(로컬 에뮬레이션) 배치 파이프라인 구축.
- 2026-07-01: 2단계 스캐폴딩 착수 — DuckDB(웨어하우스)+Airflow(LocalExecutor)+dbt(staging/marts) 구성 결정 및 파일 작성 완료. 다음은 실제 기동/검증.
- 2026-07-01: 2단계 완료 — Airflow DAG가 GCS raw 파일을 DuckDB에 적재하고 dbt로 staging/mart까지 빌드하는 것을 실제로 검증함(일별 매출/상품별 판매/재구매 고객 마트 데이터 확인). 다음 단계는 3단계(데이터 품질 체크 + 지연 모니터링) 또는 4단계(BI 대시보드) 중 선택 필요.
