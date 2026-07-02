# BI 대시보드 (Looker Studio)

DuckDB(로컬 웨어하우스)는 클라우드 서비스인 Looker Studio에서 직접 연결할 수 없다.
그래서 Airflow DAG(`shop_pipeline`)의 마지막 단계에서 dbt 마트 테이블을 `bi/exports/*.csv`로
내보내고, 그 CSV를 Google Sheets에 업로드해 Looker Studio가 Sheets 커넥터로 붙는 구조를
사용한다.

```
DuckDB(main_marts.*) --export_marts_to_csv(Airflow)--> bi/exports/*.csv
   --(수동 업로드)--> Google Sheets --(Sheets 커넥터)--> Looker Studio
```

## 1. CSV 생성 (자동)
`docker compose up`으로 파이프라인을 돌리면 `shop_pipeline` DAG 실행 시마다
`bi/exports/` 아래 3개 CSV가 최신 마트 데이터로 갱신된다.

- `fct_daily_sales.csv` — 일별 주문수/매출/평균 주문 금액
- `fct_product_sales.csv` — 상품별 판매량/매출/현재고
- `mart_user_purchase_summary.csv` — 사용자별 누적 구매액(LTV)/재구매 여부

## 2. Google Sheets 업로드 (수동, 1회 또는 필요할 때 갱신)
1. https://sheets.new 로 새 스프레드시트 생성 (예: "ecommerce-cdc-marts")
2. 시트 3개를 만들고 이름을 각각 `fct_daily_sales`, `fct_product_sales`,
   `mart_user_purchase_summary`로 지정
3. 각 시트에서 파일 > 가져오기 > 업로드로 대응하는 CSV를 "현재 시트 바꾸기" 옵션으로 가져오기

## 3. Looker Studio 대시보드 구성
1. https://lookerstudio.google.com 에서 새 보고서 생성
2. 데이터 추가 > Google Sheets 커넥터 선택 > 위에서 만든 스프레드시트/시트 3개를 각각
   데이터 소스로 연결
3. 추천 차트 구성 (CJ올리브영/커머스 BI 공고가 요구하는 "매출 대시보드" 성격에 맞춤):
   - **일별 매출 추이**: `fct_daily_sales` 기준 꺾은선 그래프 (x=order_date, y=revenue)
   - **상품별 매출 Top N**: `fct_product_sales` 기준 막대 그래프 (x=product_name, y=revenue), category로 필터/분할
   - **재구매 고객 LTV Top N**: `mart_user_purchase_summary` 기준 표 또는 막대 그래프,
     `is_repeat_customer`로 필터
   - 상단에 스코어카드(총매출/총주문수/재구매율) 추가
4. 완성된 보고서는 "공유 > 링크 보기 권한"으로 공개 링크를 만들어 GitHub README/블로그에
   첨부한다.

## 참고: 재현성에 대한 메모
1~3단계(CDC/Kafka/데이터레이크, Airflow/dbt/웨어하우스, 데이터 품질/지연 모니터링)는
`docker compose up`만으로 리뷰어가 별도 계정 없이 완전히 재현 가능하다. 이 4단계만 Looker
Studio가 클라우드 전용 서비스라 Google 계정 및 수동 업로드가 필요하다 — 프로젝트 목표대로
"GCP는 로컬 에뮬레이션 우선, 최종 산출물만 실제 클라우드"라는 원칙에 따른 설계다.
