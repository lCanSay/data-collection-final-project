from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "raw_events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",     # use "latest" for only new ones
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

for msg in consumer:
    print(msg.value)
