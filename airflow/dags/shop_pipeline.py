"""이커머스 CDC 배치 파이프라인 DAG.

1. ingest_raw_to_duckdb: 데이터레이크(fake-gcs-server)에 쌓인 CDC 이벤트 파일을 읽어
   DuckDB(로컬 BigQuery 대체 웨어하우스)의 raw 테이블에 적재한다. 적재하면서 CDC 이벤트
   발생 시각 대비 지연(latency)도 함께 측정해 pipeline_latency_log에 남긴다.
2. dbt_build: dbt로 staging(최신 상태 뷰) -> marts(일별 매출/상품별 판매/사용자 요약) 를 빌드하고 테스트한다.
3. export_marts_to_csv: 마트 테이블을 CSV로 내보낸다 (Google Sheets 업로드 -> Looker Studio 연결용).
4. data_quality_gate: latency SLA + dbt test 결과를 검사해 dq_check_log에 기록하고,
   위반 시 태스크를 실패시켜 Airflow UI에 알림을 노출한다.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from dbt_lib import run_dbt_build
from dq_lib import run_quality_gate
from export_lib import export_marts_to_csv as export_marts_to_csv_fn
from ingest_lib import ingest_all

with DAG(
    dag_id="shop_pipeline",
    description="CDC raw 적재 + dbt staging/mart 빌드",
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["portfolio", "cdc", "dbt"],
) as dag:
    ingest_raw_to_duckdb = PythonOperator(
        task_id="ingest_raw_to_duckdb",
        python_callable=ingest_all,
    )

    dbt_build = PythonOperator(
        task_id="dbt_build",
        python_callable=run_dbt_build,
    )

    export_marts_to_csv = PythonOperator(
        task_id="export_marts_to_csv",
        python_callable=export_marts_to_csv_fn,
    )

    data_quality_gate = PythonOperator(
        task_id="data_quality_gate",
        python_callable=run_quality_gate,
        # dbt_build이 실패(dbt test 실패 등)해도 latency SLA/DQ 기록은 항상 남기고 판단한다.
        trigger_rule="all_done",
    )

    ingest_raw_to_duckdb >> dbt_build >> export_marts_to_csv >> data_quality_gate
