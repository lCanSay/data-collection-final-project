# Use Python 3.11 variant so we can install pandas 2.2.x and other py3.9+ packages
FROM apache/airflow:2.8.4-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    sqlite3 \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

COPY src /opt/airflow/src
COPY airflow/dags /opt/airflow/dags
