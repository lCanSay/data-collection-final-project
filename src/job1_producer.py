import json, os, time
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv("/opt/airflow/.env")

def fetch_from_api() -> dict:
    # TODO: implement API call
    return {"ts": int(time.time()), "value": 123}

def main():
    batch_size = int(os.getenv("PRODUCE_COUNT", "1"))
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    topic = os.getenv("KAFKA_TOPIC_RAW", "raw_events")

    for _ in range(batch_size):
        msg = fetch_from_api()
        producer.send(topic, value=msg)
    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()
