# 이커머스 CDC 데이터 파이프라인

[![CI](https://github.com/donghajang213/ecommerce-cdc-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/donghajang213/ecommerce-cdc-pipeline/actions/workflows/ci.yml)

가상의 이커머스 서비스를 대상으로, CDC(Change Data Capture)부터 실시간 스트리밍,
배치 적재/모델링, 데이터 품질·지연 모니터링, BI 대시보드, CI까지 **엔드투엔드로 직접
구축하고 실제로 동작을 검증한** 데이터 엔지니어링 포트폴리오 프로젝트입니다.

📊 **[Looker Studio 대시보드 (public)](https://datastudio.google.com/reporting/6a3719fa-7d0b-4c23-83ea-257fad8f0ad9)**

## 아키텍처

```
Postgres(source) --Debezium CDC--> Kafka --lake-writer--> fake-gcs-server(데이터레이크)
    │                                                              │
    └─ simulate.py가 지속적으로 주문 생성/상태변경 발생               │
                                                                    ▼
                                          Airflow DAG(shop_pipeline, 5분 주기)
                                             1) ingest_raw_to_duckdb  (GCS -> DuckDB raw)
                                             2) dbt_build             (staging -> marts, dbt test)
                                             3) data_quality_gate     (SLA/정합성 위반 시 실패 처리)
                                             4) export_marts_to_csv   (bi/exports/*.csv)
                                                                    │
                                                    (수동, 1회) Google Sheets 업로드
                                                                    ▼
                                                          Looker Studio 대시보드
```

로컬 재현성을 위해 GCP는 fake-gcs-server(GCS 에뮬레이터)와 DuckDB(BigQuery 대체)로
로컬 에뮬레이션했고, BI 대시보드 단계만 Looker Studio가 클라우드 전용 서비스라 Google
계정으로 수동 연결합니다. 설계 배경과 각 단계의 의사결정/트러블슈팅 기록은
[PROJECT_PLAN.md](PROJECT_PLAN.md)에 전부 남겨뒀습니다.

## 기술 스택
- **CDC/스트리밍**: Postgres(logical replication), Debezium, Kafka
- **데이터레이크**: fake-gcs-server (GCS 에뮬레이터)
- **오케스트레이션**: Airflow (LocalExecutor)
- **웨어하우스/모델링**: DuckDB, dbt (staging/marts, dbt test)
- **품질/모니터링**: dbt test(정합성) + 커스텀 latency 로그/SLA 게이트
- **BI**: Google Sheets + Looker Studio
- **CI**: GitHub Actions (lint + 전체 스택 스모크 테스트)

## 로컬에서 실행하기
```bash
git clone https://github.com/donghajang213/ecommerce-cdc-pipeline.git
cd ecommerce-cdc-pipeline
docker compose up -d --build
```
몇 분 후 `shop_pipeline` DAG가 5분 주기로 자동 실행되며 `bi/exports/*.csv`가 생성됩니다.
Airflow 웹 UI: http://localhost:8080 (admin/admin)

Docker Desktop 가상화 관련 오류가 나면 Windows의 Hyper-V/Virtual Machine
Platform/WSL 기능이 켜져 있는지 확인하세요 (자세한 내용은 PROJECT_PLAN.md 트러블슈팅
참고).

## 더 읽어보기
- [PROJECT_PLAN.md](PROJECT_PLAN.md) — 목표, 타겟 채용공고 요구사항, 단계별 설계 결정과 근거, 트러블슈팅 전체 기록
- [blog/](blog/) — 단계별 개발 회고 (Tistory 포스팅용 초안)
