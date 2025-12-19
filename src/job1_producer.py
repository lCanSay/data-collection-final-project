import json
import os
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv("/opt/airflow/.env")

def log(msg: str):
    print(f"[job1_producer] {msg}", flush=True)

BASE = "https://api.electricitymaps.com/v3"

ZONES = [z.strip() for z in os.getenv("ZONES", "GB,TR,FR").split(",") if z.strip()]
TOPIC = os.getenv("KAFKA_TOPIC_RAW", "raw_events")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

TOKEN = os.getenv("ELECTRICITYMAPS_AUTH_TOKEN")
if not TOKEN:
    raise SystemExit("Missing ELECTRICITYMAPS_AUTH_TOKEN")

HEADERS = {"auth-token": TOKEN}

STATE_PATH = os.getenv("JOB1_STATE_PATH", "/opt/airflow/data/job1_state.json")


def load_state(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}


def save_state(path: str, state: dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, path)  # atomic write


def fetch(url, params):
    r = requests.get(url, headers=HEADERS, params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def state_key(zone: str, signal: str) -> str:
    return f"{zone}|{signal}"


def main():
    log(f"Starting run; zones={ZONES}, topic={TOPIC}, bootstrap={BOOTSTRAP}")
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    state = load_state(STATE_PATH)
    log(f"Loaded state entries: {len(state)}")

    def publish_if_new(zone: str, signal: str, payload: dict):
        dt = payload.get("datetime")
        if not dt:
            log(f"Skip {zone}/{signal}: missing datetime")
            return

        k = state_key(zone, signal)
        if state.get(k) == dt:
            log(f"Skip {zone}/{signal}: duplicate datetime {dt}")
            return  # duplicate for same hour -> skip

        # mark seen BEFORE saving; we will save after loop
        state[k] = dt
        log(f"Publish {zone}/{signal}: datetime={dt}")
        # producer.send(TOPIC, value={"zone": zone, "signal": signal, "payload": payload})
        f = producer.send(TOPIC, value={"zone": zone, "signal": signal, "payload": payload})
        md = f.get(timeout=10)  # forces ack / throws real error if send failed
        log(f"Sent -> topic={md.topic} partition={md.partition} offset={md.offset}")


    for zone in ZONES:
        ci = fetch(f"{BASE}/carbon-intensity/latest", {
            "zone": zone,
            "disableEstimations": "false",
            "temporalGranularity": "5_minutes", # can change to hourly
            "emissionFactorType": "lifecycle",
        })
        publish_if_new(zone, "carbon_intensity", ci)

        tl = fetch(f"{BASE}/total-load/latest", {
            "zone": zone,
            "temporalGranularity": "5_minutes",
        })
        publish_if_new(zone, "total_load", tl)

        pr = fetch(f"{BASE}/price-day-ahead/latest", {
            "zone": zone,
            "temporalGranularity": "5_minutes",
        })
        publish_if_new(zone, "price_day_ahead", pr)

    producer.flush()
    producer.close()

    save_state(STATE_PATH, state)
    log(f"Saved state entries: {len(state)}")


if __name__ == "__main__":
    main()
