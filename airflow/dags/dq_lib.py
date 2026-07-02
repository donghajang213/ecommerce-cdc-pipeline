"""데이터 품질 게이트: 파이프라인 지연(latency) SLA 체크 + dbt test 결과 검사.

CJ올리브영 공고의 "데이터 정합성/Latency 최소화" 요건을 겨냥한 모니터링/알림 단계.
검사 결과는 DuckDB의 dq_check_log 테이블에 남기고(추후 BI 대시보드 소재),
하나라도 실패하면 AirflowException을 던져 Airflow UI에서 태스크 실패로 노출한다
(= 알림. 실제 운영이라면 이 예외를 Slack/이메일 콜백에 연결하면 됨).
"""
import json
import os
from datetime import datetime, timezone

import duckdb
from airflow.exceptions import AirflowException

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/opt/airflow/warehouse/shop.duckdb")
DBT_TARGET_DIR = os.environ.get("DBT_TARGET_DIR", "/opt/airflow/dbt/target")
# DAG가 5분 주기이므로 그 2배를 SLA로 둔다 (한 배치 밀려도 알림이 안 울리게).
LATENCY_SLA_SECONDS = int(os.environ.get("LATENCY_SLA_SECONDS", "600"))


def get_duckdb_connection():
    con = duckdb.connect(DUCKDB_PATH)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS dq_check_log (
            run_ts TIMESTAMP,
            check_name VARCHAR,
            status VARCHAR,
            detail VARCHAR
        )
        """
    )
    return con


def check_latency_sla(con, run_ts) -> tuple[bool, str]:
    if run_ts is None:
        return True, "이번 배치에 신규 이벤트 없음 (latency 측정 대상 없음)"

    rows = con.execute(
        "SELECT table_name, max_latency_seconds FROM pipeline_latency_log WHERE run_ts = ?",
        [run_ts],
    ).fetchall()
    if not rows:
        return True, "이번 배치에 신규 이벤트 없음 (latency 측정 대상 없음)"

    breaches = [f"{t}({m:.0f}s)" for t, m in rows if m > LATENCY_SLA_SECONDS]
    if breaches:
        return False, f"SLA {LATENCY_SLA_SECONDS}s 초과: {', '.join(breaches)}"

    worst = max(m for _, m in rows)
    return True, f"모든 테이블 SLA 이내 (최대 latency {worst:.0f}s)"


def check_dbt_test_results() -> tuple[bool, str]:
    run_results_path = os.path.join(DBT_TARGET_DIR, "run_results.json")
    if not os.path.exists(run_results_path):
        return False, "run_results.json 없음 (dbt test 미실행)"

    with open(run_results_path, encoding="utf-8") as f:
        results = json.load(f)

    failures = [
        r["unique_id"]
        for r in results.get("results", [])
        if r.get("status") in ("fail", "error")
    ]
    if failures:
        return False, f"dbt test 실패: {', '.join(failures)}"
    return True, "dbt test 전부 통과"


def run_quality_gate() -> None:
    con = get_duckdb_connection()
    run_ts = datetime.now(timezone.utc)
    failed: list[str] = []
    try:
        latest_ingest_run_ts = con.execute(
            "SELECT max(run_ts) FROM pipeline_latency_log"
        ).fetchone()[0]

        checks = [
            ("latency_sla", check_latency_sla(con, latest_ingest_run_ts)),
            ("dbt_test", check_dbt_test_results()),
        ]

        for name, (ok, detail) in checks:
            status = "PASS" if ok else "FAIL"
            con.execute(
                "INSERT INTO dq_check_log VALUES (?, ?, ?, ?)",
                [run_ts, name, status, detail],
            )
            print(f"[dq] {name}: {status} - {detail}")
            if not ok:
                failed.append(f"{name}: {detail}")
    finally:
        con.close()

    if failed:
        raise AirflowException("데이터 품질 게이트 실패 -> " + " | ".join(failed))
