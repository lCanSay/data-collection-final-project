# Project Demo: [Watch on YouTube](https://www.youtube.com/watch?v=YcC7G6XW2X0)

# Final Project – Airflow + Kafka (Docker Compose)

This stack runs Apache Airflow (with SQLite + SequentialExecutor) and a Kafka broker. A helper service creates the `raw_events` topic on startup. Airflow runs a DAG every minute that produces a small batch of messages to Kafka.

## Prerequisites
- Docker and Docker Compose installed and running.
- Ports free: `8080` (Airflow UI), `9092` (Kafka).

## First-time setup
1) Copy environment file and adjust values as needed:
```
cp .env.example .env
```
Key vars:
- `KAFKA_BOOTSTRAP_SERVERS` (default `kafka:9092`)
- `KAFKA_TOPIC_RAW` (default `raw_events`)
- `PRODUCE_COUNT` (messages per DAG run, default 1)
- `ELECTRICITYMAPS_AUTH_TOKEN` (your api token for requests)

2) Create local folders for persisted data:
```
mkdir -p airflow/db airflow/dags airflow/logs kafka-data
```

## Build and run
Build images:
```
docker compose build --no-cache
```
Start the stack:
```
docker compose up
```
Services:
- `kafka`: Apache Kafka KRaft broker (data persisted in `./kafka-data`).
- `kafka-init`: waits for Kafka and creates `raw_events` topic (exits after success).
- `airflow-init`: initializes Airflow DB (SQLite at `./airflow/db/airflow.db`) and creates the admin user (`admin`/`admin`).
- `airflow-webserver`: Airflow UI on http://localhost:8080
- `airflow-scheduler`: runs DAGs.

If you change Python dependencies, rebuild before bringing the stack up.

## Airflow access
- URL: http://localhost:8080
- User: `admin`
- Password: `admin`

The ingestion DAG (`job1_ingestion`) is scheduled every 2 minutes and runs `/opt/airflow/src/job1_producer.py`, which sends `PRODUCE_COUNT` messages then exits.

## Kafka topic
The `raw_events` topic is created automatically by `kafka-init`. Data and topics persist in `./kafka-data` as long as you avoid removing volumes.


## Stopping and cleaning up
- Stop containers but keep data (Kafka data, Airflow DB, logs):  
  `docker compose down`
- Stop and remove all volumes/data (Kafka topics and Airflow DB will be wiped):  
  `docker compose down -v`
