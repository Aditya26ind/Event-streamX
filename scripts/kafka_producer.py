import json
import os
import sys
import time
from pathlib import Path

from kafka import KafkaProducer

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.logging_config import setup_logger
from scripts.event_generator import GenerateEvents

logger = setup_logger()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")
PRODUCE_INTERVAL_SECONDS = float(os.getenv("PRODUCE_INTERVAL_SECONDS", "2"))


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def main() -> None:
    generator = GenerateEvents()
    producer = create_producer()

    logger.info(
        "Kafka producer started",
        extra={
            "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "topic": KAFKA_TOPIC,
            "interval_seconds": PRODUCE_INTERVAL_SECONDS,
        },
    )

    while True:
        event = generator.generate_event()
        producer.send(KAFKA_TOPIC, value=event)
        producer.flush()

        logger.info(
            "Event sent to Kafka",
            extra={"topic": KAFKA_TOPIC, "event_id": event["event_id"]},
        )
        time.sleep(PRODUCE_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
