FROM apache/airflow:2.9.3-python3.11

# dbt-duckdb: dbt가 DuckDB(로컬 BigQuery 대체)에 모델을 만들 수 있게 하는 어댑터
# duckdb, google-cloud-storage: DAG의 파이썬 태스크에서 GCS 에뮬레이터의 raw 파일을 읽어 DuckDB에 적재하는 데 사용
#
# dbt-core를 안 고정하면 pip가 dbt-duckdb==1.8.1보다 훨씬 최신인 dbt-core를 딸려오는데
# ("At least one plugin is out of date with dbt-core" 경고), 이 조합에서 dbt debug/run이
# 아무 출력도 없이 즉시 죽는(exit code 2) 문제를 CI에서 실제로 겪었다. dbt-duckdb 1.8.1과
# 같은 1.8.x 계열로 dbt-core도 명시적으로 고정해서 어댑터-코어 버전을 맞춘다.
RUN pip install --no-cache-dir \
    dbt-core==1.8.9 \
    dbt-duckdb==1.8.1 \
    google-cloud-storage==2.16.0

# 볼륨 마운트 전에 미리 디렉터리를 만들어 airflow 유저 소유권으로 베이킹해둔다.
# (그래야 이름 있는 볼륨이 처음 붙을 때 이 소유권/권한을 그대로 복사해감)
RUN mkdir -p /opt/airflow/warehouse
