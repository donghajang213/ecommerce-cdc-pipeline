"""dbt 마트 테이블을 CSV로 내보내는 헬퍼.

Looker Studio는 클라우드 서비스라 로컬 DuckDB 파일에 직접 연결할 수 없다. 대신 이
CSV를 Google Sheets에 업로드하고 Looker Studio에서 그 시트를 데이터 소스로 연결하는
방식으로 BI 대시보드를 구성한다 (`bi/README.md` 참고).
"""
import csv
import os

import duckdb

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/opt/airflow/warehouse/shop.duckdb")
EXPORT_DIR = os.environ.get("EXPORT_DIR", "/opt/airflow/exports")

MART_TABLES = ["fct_daily_sales", "fct_product_sales", "mart_user_purchase_summary"]


def export_marts_to_csv() -> None:
    os.makedirs(EXPORT_DIR, exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        for table in MART_TABLES:
            rows = con.execute(f"SELECT * FROM main_marts.{table}").fetchall()
            columns = [c[0] for c in con.description]

            path = os.path.join(EXPORT_DIR, f"{table}.csv")
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)
            print(f"[export] {table}: wrote {len(rows)} rows to {path}")
    finally:
        con.close()
