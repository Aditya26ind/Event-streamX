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
KAFKA_CONNECT_RETRIES = int(os.getenv("KAFKA_CONNECT_RETRIES", "5"))
KAFKA_CONNECT_RETRY_BACKOFF_SECONDS = float(
    os.getenv("KAFKA_CONNECT_RETRY_BACKOFF_SECONDS", "3")
)
BATCH_SIZE = int(os.getenv("PRODUCER_BATCH_SIZE", "100"))  # Batch size for sending
shutdown_requested = Event()


def create_producer() -> KafkaProducer:
    last_error = None
    for attempt in range(1, KAFKA_CONNECT_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else str(k).encode("utf-8"),
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                retries=10,
                retry_backoff_ms=1000,
                max_in_flight_requests_per_connection=5,  # Increased for parallelism
                acks="all",                        # durability guarantee
                batch_size=65536,                  # 64 KB batch size for higher throughput
                linger_ms=10,                      # 10ms delay to batch more records
                compression_type="snappy",         # Better compression ratio for vast data
                buffer_memory=33554432,            # 32 MB buffer
                max_block_ms=60000,                # 60 seconds max block time
                request_timeout_ms=30000,
                delivery_timeout_ms=120000,        # 2 minutes max delivery time
                enable_idempotence=True,           # Ensure exactly-once delivery
                transactional_id=None,             # Not using transactions for simplicity
            )
            logger.info(
                "Kafka producer connected",
                extra={
                    "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
                    "topic": KAFKA_TOPIC,
                    "attempt": attempt,
                },
            )
            return producer
        except Exception as exc:
            last_error = exc
            logger.warning(
                "Kafka producer broker unavailable",
                extra={
                    "attempt": attempt,
                    "max_attempts": KAFKA_CONNECT_RETRIES,
                    "retry_in_seconds": KAFKA_CONNECT_RETRY_BACKOFF_SECONDS,
                    "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
                },
            )
            if attempt < KAFKA_CONNECT_RETRIES:
                shutdown_requested.wait(KAFKA_CONNECT_RETRY_BACKOFF_SECONDS)

        if shutdown_requested.is_set():
            break

    raise RuntimeError("Unable to create Kafka producer") from last_error


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
            "batch_size": BATCH_SIZE,
        },
    )

    batch = []
    futures = []
    try:
        while not shutdown_requested.is_set():
            event = generator.generate_event()
            batch.append(event)

            if len(batch) >= BATCH_SIZE:
                # Send batch asynchronously
                for event in batch:
                    future = producer.send(
                        KAFKA_TOPIC,
                        key=event["user_id"],
                        value=event,
                    )
                    future.add_callback(log_delivery_success, event["event_id"])
                    future.add_errback(log_delivery_error, event["event_id"])
                    futures.append(future)

                logger.info(
                    "Batch sent to Kafka",
                    extra={
                        "topic": KAFKA_TOPIC,
                        "batch_size": len(batch)
                    },
                )
                batch = []

                # Clean up completed futures to prevent memory buildup
                futures = [f for f in futures if not f.is_done]

            # Small delay between generations
            shutdown_requested.wait(0.01)  # 10ms delay

        # Send remaining events in batch
        if batch:
            for event in batch:
                future = producer.send(
                    KAFKA_TOPIC,
                    key=event["user_id"],
                    value=event,
                )
                future.add_callback(log_delivery_success, event["event_id"])
                future.add_errback(log_delivery_error, event["event_id"])
                futures.append(future)

            logger.info(
                "Final batch sent to Kafka",
                extra={
                    "topic": KAFKA_TOPIC,
                    "batch_size": len(batch)
                },
            )

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        shutdown_requested.set()
    finally:
        logger.info("Shutting down Kafka producer")
        # Wait for all futures to complete
        for future in futures:
            try:
                future.get(timeout=10)
            except Exception as e:
                logger.error("Error waiting for future", exc_info=e)
        producer.flush()
        producer.close()
        logger.info("Kafka producer closed cleanly")


if __name__ == "__main__":
    main()
