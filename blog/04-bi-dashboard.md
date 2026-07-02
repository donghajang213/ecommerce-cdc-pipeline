# [이커머스 CDC 파이프라인 #4] Looker Studio로 매출 대시보드 만들기

## 이번 편의 목표

[3편](03-data-quality-latency.md)까지 CDC → Kafka → 데이터레이크 → Airflow/dbt →
DuckDB 웨어하우스 → 데이터 품질/지연 모니터링까지 완성했다. 이번 편에서는 커머스
BI 공고가 요구한 "Tableau/Looker Studio 대시보드"를 만든다.

## 클라우드 전용 서비스 앞에서 로컬 에뮬레이션 전략이 끝나는 지점

1~3단계는 전부 "로컬 에뮬레이션 우선, 나중에 실제 GCP로 전환"이라는 원칙으로
진행했다 — GCS는 `fake-gcs-server`, BigQuery는 DuckDB로 대체했다. 하지만 Looker
Studio는 애초에 클라우드에서만 동작하는 SaaS라 로컬 DuckDB 파일에 직접 연결할
방법이 없다. 여기서 처음으로 "로컬로 완전히 재현 가능한 파이프라인"이라는 원칙과
"실제 BI 도구를 써본 경험"이라는 목표가 충돌했다.

선택한 절충안은 이렇다.

```
DuckDB(main_marts.*) --export_marts_to_csv(Airflow)--> bi/exports/*.csv
   --(수동 업로드)--> Google Sheets --(Sheets 커넥터)--> Looker Studio
```

1~3단계는 여전히 `docker compose up`만으로 완전히 재현 가능하게 남겨두고, 4단계
마지막 "클라우드에 실제로 데이터를 올리고 시각화하는" 부분만 수동 개입이 필요한
단계로 명확히 분리했다. Google Sheets API로 자동 업로드하는 방법도 고려했지만,
그러려면 GCP 서비스 계정을 만들고 키 JSON을 발급받는 선행 작업이 필요해서 —
이 프로젝트의 "별도 계정 없이 재현 가능"이라는 원칙에 어긋난다고 판단해 CSV
export + 수동 업로드로 단순화했다.

## 구현

`export_marts_to_csv`라는 Airflow 태스크를 DAG 마지막 부분(`dbt_build` 다음,
`data_quality_gate` 이전)에 추가했다.

```python
MART_TABLES = ["fct_daily_sales", "fct_product_sales", "mart_user_purchase_summary"]

def export_marts_to_csv() -> None:
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    for table in MART_TABLES:
        rows = con.execute(f"SELECT * FROM main_marts.{table}").fetchall()
        columns = [c[0] for c in con.description]
        # bi/exports/<table>.csv 로 기록
```

`docker-compose.yml`에는 `./bi/exports:/opt/airflow/exports` 볼륨을 추가해서,
컨테이너 안에서 생성된 CSV를 호스트에서 바로 열어볼 수 있게 했다. 이 DAG를 돌릴
때마다 최신 마트 데이터로 CSV가 갱신된다.

## Google Sheets → Looker Studio 연결

1. Google Sheets에 새 스프레드시트를 만들고, 마트 개수만큼(3개) 시트를 나눠 CSV를
   업로드했다.
2. Looker Studio에서 새 보고서를 만들고, 데이터 소스로 Google Sheets 커넥터를
   붙였다.
3. 구성한 차트:
   - 일별 매출 추이 (꺾은선 그래프, `fct_daily_sales`)
   - 상품별 매출 Top N (막대 그래프, `fct_product_sales`, 카테고리 필터)
   - 재구매 고객 LTV Top N (표, `mart_user_purchase_summary`)
   - 총매출/총주문수/재구매율 스코어카드

## 회고: 왜 이 분리가 의미 있었나

돌이켜보면 이 4단계는 기술적으로 가장 단순한 단계였지만("CSV 내보내고 시트에
업로드"), 오히려 그래서 더 의미가 있었다. 실제 데이터 엔지니어링 업무에서도
"완전 자동화된 파이프라인"과 "사람이 마지막에 확인/배포하는 지점"이 공존하는
경우가 많다. 어디까지 자동화하고 어디서 사람이 개입할지를 명시적으로 설계 문서에
남기는 것 자체가 이 프로젝트에서 배운 점이다.

## 마무리

여기까지 CDC(Debezium+Kafka) → 데이터레이크(GCS 에뮬레이터) →
Airflow/dbt(DuckDB 웨어하우스) → 데이터 품질/지연 모니터링 → BI 대시보드까지
4단계로 이어지는 이커머스 데이터 파이프라인을 완성했다. 전체 코드는
[GitHub](https://github.com/donghajang213/ecommerce-cdc-pipeline)에서 확인할
수 있다.
