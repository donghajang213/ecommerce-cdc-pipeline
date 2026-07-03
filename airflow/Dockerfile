FROM apache/airflow:2.9.3-python3.11

# dbt-duckdb: dbt가 DuckDB(로컬 BigQuery 대체)에 모델을 만들 수 있게 하는 어댑터
# duckdb, google-cloud-storage: DAG의 파이썬 태스크에서 GCS 에뮬레이터의 raw 파일을 읽어 DuckDB에 적재하는 데 사용
#
# dbt-core를 dbt-duckdb==1.8.1과 같은 1.8.x 계열로 명시적으로 고정해서 어댑터-코어
# 버전을 맞춘다 (안 그러면 pip가 훨씬 최신 dbt-core를 딸려와서 "At least one plugin
# is out of date with dbt-core" 경고가 뜬다).
RUN pip install --no-cache-dir \
    dbt-core==1.8.9 \
    dbt-duckdb==1.8.1 \
    google-cloud-storage==2.16.0

# 볼륨 마운트 전에 미리 디렉터리를 만들어 airflow 유저 소유권으로 베이킹해둔다.
# (그래야 이름 있는 볼륨이 처음 붙을 때 이 소유권/권한을 그대로 복사해감)
#
# dbt_target/dbt_logs는 dbt_project.yml의 target-path/log-path가 가리키는 곳으로,
# 바인드 마운트되는 /opt/airflow/dbt(호스트의 ./dbt) 밖에 일부러 둔다. GitHub Actions
# 처럼 매번 새로 체크아웃하는 환경에서는 ./dbt/target, ./dbt/logs가 호스트에 아직
# 없어서 Docker가 root 소유로 자동 생성해버리고, 컨테이너의 비-root airflow 유저가
# 못 써서 dbt가 로거 초기화 단계에서 "Permission denied"로 조용히 실패했었다.
RUN mkdir -p /opt/airflow/warehouse /opt/airflow/dbt_target /opt/airflow/dbt_logs
