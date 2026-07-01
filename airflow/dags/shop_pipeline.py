"""이커머스 CDC 배치 파이프라인 DAG.

1. ingest_raw_to_duckdb: 데이터레이크(fake-gcs-server)에 쌓인 CDC 이벤트 파일을 읽어
   DuckDB(로컬 BigQuery 대체 웨어하우스)의 raw 테이블에 적재한다.
2. dbt_build: dbt로 staging(최신 상태 뷰) -> marts(일별 매출/상품별 판매/사용자 요약) 를 빌드하고 테스트한다.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_lib import ingest_all

DBT_PROJECT_DIR = "/opt/airflow/dbt"

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

    dbt_build = BashOperator(
        task_id="dbt_build",
        cwd=DBT_PROJECT_DIR,
        bash_command=(
            f"dbt run --profiles-dir {DBT_PROJECT_DIR} "
            f"&& dbt test --profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    ingest_raw_to_duckdb >> dbt_build
