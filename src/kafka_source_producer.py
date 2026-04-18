import json
import os
import time
from pathlib import Path

from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flights.ingest.request")
SOURCE_DIR = Path(os.getenv("SOURCE_DIR", "/app/data/source"))
STATE_FILE = Path(os.getenv("STATE_FILE", "/app/data/kafka/producer_state.json"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "10"))


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {"sent_files": []}
    with STATE_FILE.open("r", encoding="utf-8") as file:
        return json.load(file)


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with STATE_FILE.open("w", encoding="utf-8") as file:
        json.dump(state, file)


def build_event(file_path: Path) -> dict:
    stat = file_path.stat()
    return {
        "event_id": f"{file_path.name}:{int(stat.st_mtime)}:{stat.st_size}",
        "file_name": file_path.name,
        "source_path": str(file_path),
        "file_size": stat.st_size,
        "created_at": int(time.time()),
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    while True:
        state = load_state()
        sent_files = set(state.get("sent_files", []))
        current_files = sorted(SOURCE_DIR.glob("*.csv"))

        if current_files and not sent_files:
            state["sent_files"] = sorted(file_path.name for file_path in current_files)
            save_state(state)
            print(f"Bootstrapped state with {len(current_files)} existing files")
            time.sleep(POLL_SECONDS)
            continue

        for file_path in current_files:
            if file_path.name in sent_files:
                continue

            event = build_event(file_path)
            producer.send(KAFKA_TOPIC, value=event)
            producer.flush()

            sent_files.add(file_path.name)
            state["sent_files"] = sorted(sent_files)
            save_state(state)
            print(f"Published event for {file_path.name}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
