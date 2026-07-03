"""dbt를 서브프로세스(CLI)가 아니라 파이썬 API(dbtRunner)로 같은 프로세스 안에서 직접 호출한다.

BashOperator로 `dbt` CLI를 서브프로세스 실행하면 GitHub Actions 호스팅 러너에서만
재현되는 원인 불명의 즉시 크래시(exit code 2, 로그 한 줄도 없이 dbt 자체 로거 초기화
전에 죽음)를 겪었다. 로컬에서는 동일 버전 조합으로 전혀 재현되지 않았고, dbt-core/
dbt-duckdb 버전을 맞추거나 /dev/shm을 늘려도 고쳐지지 않아 서브프로세스 실행 경로
자체가 의심스러웠다. ingest/export/dq 태스크는 전부 PythonOperator(같은 프로세스 내
직접 호출)라 CI에서도 문제없이 동작했으므로, dbt도 같은 방식(dbtRunner)으로 맞춘다.
"""
import os

from airflow.exceptions import AirflowException
from dbt.cli.main import dbtRunner

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")


def run_dbt_build() -> None:
    runner = dbtRunner()
    common_args = ["--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR]

    run_result = runner.invoke(["run", *common_args])
    if not run_result.success:
        raise AirflowException(f"dbt run 실패: {run_result.exception or '모델 빌드 실패'}")

    test_result = runner.invoke(["test", *common_args])
    if not test_result.success:
        # 개별 테스트 실패 상세는 data_quality_gate가 target/run_results.json을 읽어서 보고한다.
        raise AirflowException(f"dbt test 실패: {test_result.exception or '하나 이상의 테스트 실패'}")
