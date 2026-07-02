"""fake-gcs-server(raw 데이터레이크)에 쌓인 Debezium CDC 이벤트를 읽어서
DuckDB(로컬 BigQuery 대체 웨어하우스)의 raw 테이블로 적재하는 헬퍼 함수들.

Debezium 이벤트 한 줄(JSON) 예시:
{"before": null, "after": {...컬럼값...}, "op": "r", "ts_ms": 169..., ...}
- op: r(스냅샷 read) / c(insert) / u(update) / d(delete)
- op == "d" 이면 after가 null이라 before를 사용
"""
import json
import os
import statistics
from datetime import datetime, timezone

import duckdb
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage

GCS_ENDPOINT = os.environ.get("GCS_ENDPOINT", "http://fake-gcs-server:4443")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "dl-raw")
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/opt/airflow/warehouse/shop.duckdb")

# 테이블별로 어떤 컬럼이 마이크로초 단위 타임스탬프(Debezium MicroTimestamp)인지 정의
TABLES = {
    "users": {
        "raw_table": "users_raw",
        "timestamp_cols": ["signup_at", "updated_at"],
        "ddl": """
            CREATE TABLE IF NOT EXISTS users_raw (
                user_id BIGINT,
                email VARCHAR,
                full_name VARCHAR,
                signup_at TIMESTAMP,
                updated_at TIMESTAMP,
                __op VARCHAR,
                __ts_ms BIGINT,
                __seq BIGINT,
                __deleted BOOLEAN
            )
        """,
    },
    "products": {
        "raw_table": "products_raw",
        "timestamp_cols": ["created_at", "updated_at"],
        "ddl": """
            CREATE TABLE IF NOT EXISTS products_raw (
                product_id BIGINT,
                sku VARCHAR,
                name VARCHAR,
                category VARCHAR,
                price DOUBLE,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                __op VARCHAR,
                __ts_ms BIGINT,
                __seq BIGINT,
                __deleted BOOLEAN
            )
        """,
    },
    "inventory": {
        "raw_table": "inventory_raw",
        "timestamp_cols": ["updated_at"],
        "ddl": """
            CREATE TABLE IF NOT EXISTS inventory_raw (
                product_id BIGINT,
                quantity BIGINT,
                updated_at TIMESTAMP,
                __op VARCHAR,
                __ts_ms BIGINT,
                __seq BIGINT,
                __deleted BOOLEAN
            )
        """,
    },
    "orders": {
        "raw_table": "orders_raw",
        "timestamp_cols": ["created_at", "updated_at"],
        "ddl": """
            CREATE TABLE IF NOT EXISTS orders_raw (
                order_id BIGINT,
                user_id BIGINT,
                status VARCHAR,
                total_amount DOUBLE,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                __op VARCHAR,
                __ts_ms BIGINT,
                __seq BIGINT,
                __deleted BOOLEAN
            )
        """,
    },
    "order_items": {
        "raw_table": "order_items_raw",
        "timestamp_cols": ["created_at"],
        "ddl": """
            CREATE TABLE IF NOT EXISTS order_items_raw (
                order_item_id BIGINT,
                order_id BIGINT,
                product_id BIGINT,
                quantity BIGINT,
                unit_price DOUBLE,
                created_at TIMESTAMP,
                __op VARCHAR,
                __ts_ms BIGINT,
                __seq BIGINT,
                __deleted BOOLEAN
            )
        """,
    },
}


def get_storage_client():
    return storage.Client(
        project="local-dev",
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": GCS_ENDPOINT},
    )


def get_duckdb_connection():
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS _raw_ingest_log (
            object_name VARCHAR PRIMARY KEY,
            loaded_at TIMESTAMP DEFAULT now()
        )
        """
    )
    # Debezium ts_ms는 밀리초 단위라 짧은 시간 안에 벌어진 이벤트(예: 주문 생성 직후 총액
    # 업데이트)는 값이 동률일 수 있다. staging 뷰에서 "최신 상태"를 안정적으로 고르려면
    # ts_ms만으로는 부족해서, 적재 순서를 보장하는 별도 시퀀스(__seq)를 함께 남긴다.
    con.execute("CREATE SEQUENCE IF NOT EXISTS event_seq START 1")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_latency_log (
            run_ts TIMESTAMP,
            table_name VARCHAR,
            event_count BIGINT,
            avg_latency_seconds DOUBLE,
            max_latency_seconds DOUBLE,
            p95_latency_seconds DOUBLE
        )
        """
    )
    return con


def _row_from_event(event: dict, timestamp_cols: list[str]) -> dict | None:
    op = event.get("op")
    payload = event.get("after") or event.get("before")
    if payload is None:
        return None

    row = dict(payload)
    for col in timestamp_cols:
        if row.get(col) is not None:
            row[col] = datetime.fromtimestamp(row[col] / 1_000_000, tz=timezone.utc)
    row["__op"] = op
    row["__ts_ms"] = event.get("ts_ms")
    row["__deleted"] = op == "d"
    return row


def _record_latency(con, run_ts: datetime, table: str, latencies: list[float]) -> None:
    """CDC 이벤트 발생 시각(ts_ms)부터 이번 배치에서 실제 적재되기까지 걸린 지연(초)을 집계해 기록.
    지연이 크면(=SLA 초과) data_quality_gate 태스크가 이를 감지해 Airflow 태스크를 실패시킨다."""
    latencies.sort()
    p95_idx = min(len(latencies) - 1, int(len(latencies) * 0.95))
    con.execute(
        "INSERT INTO pipeline_latency_log VALUES (?, ?, ?, ?, ?, ?)",
        [
            run_ts,
            table,
            len(latencies),
            statistics.mean(latencies),
            max(latencies),
            latencies[p95_idx],
        ],
    )


def ingest_table(con, table: str, run_ts: datetime) -> int:
    """지정한 테이블의 새 raw 파일들을 GCS 에뮬레이터에서 읽어 DuckDB에 적재. 적재된 이벤트 수 반환."""
    cfg = TABLES[table]
    con.execute(cfg["ddl"])

    client = get_storage_client()
    bucket = client.bucket(GCS_BUCKET)
    blobs = list(bucket.list_blobs(prefix=f"raw/{table}/"))

    loaded_names = {
        r[0] for r in con.execute("SELECT object_name FROM _raw_ingest_log").fetchall()
    }
    new_blobs = sorted(
        (b for b in blobs if b.name not in loaded_names), key=lambda b: b.name
    )
    if not new_blobs:
        return 0

    total_rows = 0
    latencies: list[float] = []
    for blob in new_blobs:
        content = blob.download_as_text()
        rows = []
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            row = _row_from_event(event, cfg["timestamp_cols"])
            if row is not None:
                row["__seq"] = con.execute("SELECT nextval('event_seq')").fetchone()[0]
                rows.append(row)
                if event.get("ts_ms") is not None:
                    event_time = datetime.fromtimestamp(event["ts_ms"] / 1000, tz=timezone.utc)
                    latencies.append((run_ts - event_time).total_seconds())

        if rows:
            columns = list(rows[0].keys())
            placeholders = ", ".join(["?"] * len(columns))
            col_list = ", ".join(columns)
            values = [[row[c] for c in columns] for row in rows]
            con.executemany(
                f"INSERT INTO {cfg['raw_table']} ({col_list}) VALUES ({placeholders})",
                values,
            )
            total_rows += len(rows)

        con.execute("INSERT INTO _raw_ingest_log (object_name) VALUES (?)", [blob.name])

    if latencies:
        _record_latency(con, run_ts, table, latencies)

    return total_rows


def ingest_all() -> None:
    con = get_duckdb_connection()
    run_ts = datetime.now(timezone.utc)
    try:
        for table in TABLES:
            n = ingest_table(con, table, run_ts)
            print(f"[ingest] {table}: loaded {n} new events")
    finally:
        con.close()
