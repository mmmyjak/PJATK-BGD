import json
import os
import re
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flights.ingest.request")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "airflow-trigger-consumer")
AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
AIRFLOW_DAG_ID = os.getenv("AIRFLOW_DAG_ID", "medallion_airline_pipeline_kafka")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")


def create_airflow_session() -> requests.Session:
    session = requests.Session()
    login_response = session.get(f"{AIRFLOW_BASE_URL}/auth/login/", timeout=15)
    token_match = re.search(r'name="csrf_token".*?value="([^"]+)"', login_response.text)

    if not token_match:
        raise RuntimeError("Could not retrieve Airflow login CSRF token")

    response = session.post(
        f"{AIRFLOW_BASE_URL}/auth/login/",
        data={
            "csrf_token": token_match.group(1),
            "username": AIRFLOW_USERNAME,
            "password": AIRFLOW_PASSWORD,
        },
        timeout=15,
        allow_redirects=True,
    )

    if response.status_code != 200:
        raise RuntimeError(f"Airflow login failed with status {response.status_code}")

    return session


def trigger_dag(session: requests.Session, event: dict) -> bool:
    dag_run_id = f"kafka_{event['event_id'].replace(':', '_')}"
    url = f"{AIRFLOW_BASE_URL}/api/v2/dags/{AIRFLOW_DAG_ID}/dagRuns"
    payload = {
        "dag_run_id": dag_run_id,
        "logical_date": datetime.now(timezone.utc).isoformat(),
        "conf": {
            "event_id": event["event_id"],
            "file_name": event["file_name"],
            "source_path": event.get("source_path", ""),
        },
    }

    response = session.post(
        url,
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=15,
    )

    if response.status_code in (200, 201, 409):
        return True

    print(f"Failed to trigger DAG: {response.status_code} {response.text}")
    return False


def main() -> None:
    session = create_airflow_session()
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    while True:
        for message in consumer:
            event = message.value
            if not event.get("event_id") or not event.get("file_name"):
                print(f"Skipping invalid event: {event}")
                consumer.commit()
                continue

            if trigger_dag(session, event):
                print(f"Triggered DAG for {event['file_name']}")
                consumer.commit()
            else:
                time.sleep(5)


if __name__ == "__main__":
    main()
