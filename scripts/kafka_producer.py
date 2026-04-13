import json
import os
import random
import signal
import sys
import time
from pathlib import Path
from threading import Event

from kafka import KafkaProducer

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.logging_config import setup_logger
from scripts.event_generator import GenerateEvents

logger = setup_logger()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS",
                                    "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")
PRODUCE_INTERVAL_SECONDS = random.uniform(1, 3)
shutdown_requested = Event()


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        retreis=5,
        retry_backoff_ms=1000,
        max_in_flight_requests_per_connection=1,
    )


def request_shutdown(signum, _frame) -> None:
    logger.info("Shutdown signal received", extra={"signal": signum})
    shutdown_requested.set()


def log_delivery_success(record_metadata, event_id: str) -> None:
    logger.info(
        "Event delivered",
        extra={
            "topic": record_metadata.topic,
            "partition": record_metadata.partition,
            "offset": record_metadata.offset,
            "event_id": event_id,
        },
    )


def log_delivery_error(exc: Exception, event_id: str) -> None:
    logger.error(
        "Failed to deliver event",
        exc_info=exc,
        extra={
            "event_id": event_id,
            "topic": KAFKA_TOPIC
        },
    )


def main() -> None:
    generator = GenerateEvents()
    producer = create_producer()
    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)

    logger.info(
        "Kafka producer started",
        extra={
            "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "topic": KAFKA_TOPIC,
            "interval_seconds": PRODUCE_INTERVAL_SECONDS,
        },
    )

    try:
        while not shutdown_requested.is_set():
            event = generator.generate_event()
            future = producer.send(
                KAFKA_TOPIC,
                key=event["user_id"].encode(),
                value=event,
            )
            future.add_callback(log_delivery_success, event["event_id"])
            future.add_errback(log_delivery_error, event["event_id"])

            logger.info(
                "Event sent to Kafka",
                extra={
                    "topic": KAFKA_TOPIC,
                    "event_id": event["event_id"]
                },
            )
            shutdown_requested.wait(PRODUCE_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        shutdown_requested.set()
    finally:
        logger.info("Shutting down Kafka producer")
        producer.flush()
        producer.close()
        logger.info("Kafka producer closed cleanly")


if __name__ == "__main__":
    main()
