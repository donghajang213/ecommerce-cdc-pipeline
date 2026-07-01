"""Debezium CDC 토픽(shop.public.*)을 구독해서 데이터레이크(fake-gcs-server)에
테이블/날짜별 JSONL 파일로 적재하는 컨슈머 (raw/bronze layer)."""
import os
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage
from google.cloud.exceptions import Conflict

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
GCS_ENDPOINT = os.environ.get("GCS_ENDPOINT", "http://fake-gcs-server:4443")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "dl-raw")
TOPIC_PATTERN = os.environ.get("TOPIC_PATTERN", "^shop\\..*")

FLUSH_INTERVAL_SECONDS = 10
FLUSH_MAX_MESSAGES = 50


def get_storage_client():
    return storage.Client(
        project="local-dev",
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": GCS_ENDPOINT},
    )


def ensure_bucket(client):
    try:
        client.create_bucket(GCS_BUCKET)
        print(f"Created bucket {GCS_BUCKET}")
    except Conflict:
        pass


def table_name_from_topic(topic: str) -> str:
    # e.g. "shop.public.orders" -> "orders"
    return topic.split(".")[-1]


def flush(bucket, buffers):
    for table, lines in list(buffers.items()):
        if not lines:
            continue
        now = datetime.now(timezone.utc)
        path = f"raw/{table}/dt={now.strftime('%Y-%m-%d')}/{now.strftime('%H%M%S_%f')}.jsonl"
        blob = bucket.blob(path)
        blob.upload_from_string("\n".join(lines), content_type="application/json")
        print(f"Wrote {len(lines)} records to gs://{bucket.name}/{path}")
        buffers[table] = []


def main():
    client = get_storage_client()
    ensure_bucket(client)
    bucket = client.bucket(GCS_BUCKET)

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "lake-writer",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC_PATTERN])

    buffers: dict[str, list[str]] = {}
    last_flush = time.time()

    print("lake-writer started, waiting for messages...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    # 패턴 구독 시 아직 매칭되는 토픽이 없으면 일시적으로 발생 (Debezium이
                    # 초기 스냅샷을 마치고 토픽을 만들 때까지). 치명적이지 않으니 재시도.
                    if msg.error().code() in (
                        KafkaError.UNKNOWN_TOPIC_OR_PART,
                        KafkaError._PARTITION_EOF,
                    ):
                        continue
                    raise KafkaException(msg.error())
                table = table_name_from_topic(msg.topic())
                value = msg.value()
                if value is not None:
                    buffers.setdefault(table, []).append(value.decode("utf-8"))

            due_by_time = time.time() - last_flush >= FLUSH_INTERVAL_SECONDS
            due_by_size = any(len(v) >= FLUSH_MAX_MESSAGES for v in buffers.values())
            if due_by_time or due_by_size:
                flush(bucket, buffers)
                last_flush = time.time()
    except KeyboardInterrupt:
        pass
    finally:
        flush(bucket, buffers)
        consumer.close()


if __name__ == "__main__":
    main()
