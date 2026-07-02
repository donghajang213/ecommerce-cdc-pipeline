# [이커머스 CDC 파이프라인 #1] Debezium + Kafka로 실시간 CDC 데이터레이크 만들기

## 왜 이 프로젝트를 시작했나

데이터 엔지니어로 이직을 준비하면서 타겟 채용공고 3건을 분석했다. CJ올리브영은 "Kafka,
CDC(OGG/Debezium), 실시간 스트리밍 파이프라인, 이기종 DB 동기화, 데이터 정합성/Latency
최소화"를 요구했고, 넥슨과 커머스 BI 공고는 "Airflow/dbt 기반 배치 파이프라인, DW/데이터마트
모델링, 대시보드"를 요구했다. 세 공고를 하나의 엔드투엔드 파이프라인으로 커버할 수 있는
포트폴리오를 만들기로 했다.

도메인은 이커머스 주문 데이터로 정했다. 주문 생성/상태 변경/재고 차감이 자연스럽게 CDC로
캡처되고(CJ 요건), 결과적으로 매출 마트/데이터마트로 이어져서(넥슨/BI 요건) 세 공고를 모두
아우를 수 있다고 판단했다.

## 아키텍처

```
Postgres(source: users/products/orders/order_items/inventory)
   → simulate.py (지속적 주문 생성/상태변경/재고차감 → CDC 이벤트 유발)
   → Debezium (Kafka Connect) → Kafka topics (topic per table)
   → lake-writer consumer → fake-gcs-server bucket (raw/<table>/dt=YYYY-MM-DD/*.jsonl)
```

GCP는 처음부터 실제 계정을 쓰지 않고 로컬 에뮬레이션으로 시작했다. GCS는
`fake-gcs-server`, BigQuery는 나중에 DuckDB로 대체할 계획이다. 이유는 두 가지다.

1. 리뷰어가 별도 클라우드 계정 없이 `docker compose up` 한 줄로 전체 파이프라인을 재현할 수
   있어야 한다.
2. 로컬에서 아키텍처를 검증한 뒤 실제 GCP로 전환하면, "왜 이 구조를 선택했는지"를 비용/속도
   손해 없이 충분히 실험하고 결정할 수 있다.

시드 데이터도 Kaggle 공개 데이터셋을 받는 대신 Faker로 직접 생성하기로 했다. 같은 이유로,
외부 계정이나 다운로드 없이 완전히 자체 완결적인 리포지토리를 만들고 싶었다.

## 구현

레포 구조:

```
docker-compose.yml
source-db/init/*.sql       (스키마)
seed/seed.py                (Faker 초기 데이터)
simulate/simulate.py         (지속적 CDC 이벤트 생성기)
debezium/register-connector.json
lake-writer/consumer.py, Dockerfile, requirements.txt
```

`simulate.py`는 무한 루프를 돌며 무작위로 주문 생성/상태 전이/재고-가격 변경 중 하나를
실행한다. 이 변경들이 Postgres의 logical replication(`wal_level=logical`)을 통해
Debezium이 캡처할 수 있는 CDC 이벤트가 된다. Debezium(Kafka Connect)이 테이블별로
Kafka 토픽(`shop.public.<table>`)에 변경 이벤트를 쏘고, `lake-writer`가 그 토픽들을
구독해서 `fake-gcs-server` 버킷에 JSONL로 쌓는다.

## 트러블슈팅

**Docker Desktop "Virtualization support not detected"**
Windows의 Hyper-V/Virtual Machine Platform/WSL 기능이 꺼져 있어서 발생했다. BIOS
가상화는 이미 켜져 있었는데(`systeminfo`의 Hyper-V Requirements로 확인) 정작
Windows 기능이 꺼져 있던 케이스. `Enable-WindowsOptionalFeature`로 활성화하고
재시작해서 해결했다.

**`debezium/connect:2.7` 이미지 태그가 없다**
Docker Hub에는 `2.7`처럼 마이너 버전만 있는 태그가 없고, `2.7.3.Final`처럼 정확한 패치
버전 태그만 존재했다. 이런 건 태그 목록을 직접 확인하지 않으면 계속 pull 실패로 헤매게
된다.

**lake-writer가 `UNKNOWN_TOPIC_OR_PART`로 죽음**
정규식 패턴(`^shop\..*`)으로 토픽을 구독했는데, 아직 매칭되는 토픽이 하나도 없는 시점에
컨슈머를 poll하면 이 에러가 난다. 원래 이 에러를 그냥 raise하도록 짜서 컨테이너가
크래시 루프를 탔다. Kafka Connect가 커넥터를 등록하고 토픽을 만들기까지 시간이 걸리는
걸 감안해서, 이 에러 코드는 무시하고 계속 poll하도록 고쳤다. "일시적으로 있을 수 있는
정상 상태"와 "진짜 에러"를 구분하는 게 컨슈머 코드에서 은근히 중요하다는 걸 배웠다.

**컨테이너 로그가 안 보임**
Python 프로세스가 파이프로 리다이렉트되면(`docker logs`가 파이프를 통해 stdout을
가져가는 상황) 기본적으로 블록 버퍼링 모드로 전환돼서 로그가 안 찍힌다. 터미널에
직접 붙어있을 때는 라인 버퍼링이라 문제가 없었는데, 도커 환경에서만 재현됐다.
`Dockerfile`에 `ENV PYTHONUNBUFFERED=1` 한 줄 추가로 해결.

## 검증

`kafka-console-consumer`로 토픽에 실제 CDC 이벤트(`op=r` 스냅샷, `op=u` 업데이트 등)가
찍히는 걸 확인했고, `lake-writer` 로그에서 `Wrote N records to gs://...`를 확인한 뒤,
`curl http://localhost:4443/storage/v1/b/dl-raw/o`로 실제 GCS 에뮬레이터 오브젝트가
생성된 걸 확인했다. Postgres에서 발생한 변경이 몇 초 안에 데이터레이크의 JSONL 파일로
나타나는 걸 보면서, CDC 파이프라인이 실제로 "살아있다"는 감각을 처음 느꼈다.

## 다음 편

다음 편에서는 이 raw 데이터를 Airflow + dbt + DuckDB(로컬 BigQuery 대체)로 가공해서
일별 매출/상품별 판매 마트를 만드는 과정을 다룬다.
