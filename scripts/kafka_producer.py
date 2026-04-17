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
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "events-dlq")
PRODUCE_INTERVAL_SECONDS = random.uniform(1, 3)
KAFKA_CONNECT_RETRIES = int(os.getenv("KAFKA_CONNECT_RETRIES", "5"))
KAFKA_CONNECT_RETRY_BACKOFF_SECONDS = float(
    os.getenv("KAFKA_CONNECT_RETRY_BACKOFF_SECONDS", "3")
)
BATCH_SIZE = int(os.getenv("PRODUCER_BATCH_SIZE", "100"))  # Batch size for sending
shutdown_requested = Event()


class ProducerMetrics:
    def __init__(self):
        self.events_sent = 0
        self.batches_sent = 0
        self.send_failures = 0
        self.dead_letter_sent = 0
        self.throughput_events_per_sec = 0
        self.last_measurement_time = time.time()

    def increment_events(self, count: int):
        self.events_sent += count

    def increment_batches(self):
        self.batches_sent += 1

    def increment_failures(self):
        self.send_failures += 1

    def increment_dead_letter(self):
        self.dead_letter_sent += 1

    def calculate_throughput(self) -> float:
        current_time = time.time()
        elapsed = current_time - self.last_measurement_time
        if elapsed > 0:
            self.throughput_events_per_sec = self.events_sent / elapsed
        self.last_measurement_time = current_time
        return self.throughput_events_per_sec

    def log_metrics(self):
        logger.info(
            "Producer metrics",
            extra={
                "events_sent": self.events_sent,
                "batches_sent": self.batches_sent,
                "send_failures": self.send_failures,
                "dead_letter_sent": self.dead_letter_sent,
                "throughput_eps": round(self.calculate_throughput(), 2),
            },
        )


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


def send_batch_with_retry(producer: KafkaProducer, batch: list, metrics: ProducerMetrics, max_retries: int = 3) -> bool:
    """Send batch with retry logic."""
    for attempt in range(max_retries):
        try:
            futures = []
            for event in batch:
                future = producer.send(
                    KAFKA_TOPIC,
                    key=event["user_id"],
                    value=event,
                )
                future.add_callback(log_delivery_success, event["event_id"])
                future.add_errback(log_delivery_error, event["event_id"])
                futures.append(future)

            # Wait for all sends in batch
            for future in futures:
                future.get(timeout=30)

            metrics.increment_events(len(batch))
            metrics.increment_batches()
            logger.info(
                "Batch sent successfully",
                extra={
                    "topic": KAFKA_TOPIC,
                    "batch_size": len(batch),
                    "attempt": attempt + 1,
                },
            )
            return True

        except Exception as exc:
            logger.warning(
                "Batch send failed, retrying",
                exc_info=exc,
                extra={
                    "batch_size": len(batch),
                    "attempt": attempt + 1,
                    "max_retries": max_retries,
                },
            )
            if attempt < max_retries - 1:
                time.sleep(1 * (2 ** attempt))  # Exponential backoff

    logger.error(
        "Batch send failed after all retries",
        extra={"batch_size": len(batch)},
    )
    return False


def send_to_dlq(producer: KafkaProducer, failed_events: list, metrics: ProducerMetrics) -> None:
    """Send failed events to dead-letter queue."""
    dlq_message = {
        "failed_events": failed_events,
        "error": "Failed to send to main topic after retries",
        "timestamp": time.time(),
        "producer_topic": KAFKA_TOPIC,
    }

    try:
        future = producer.send(
            KAFKA_DLQ_TOPIC,
            key=str(time.time()),  # Use timestamp as key
            value=dlq_message,
        )
        future.get(timeout=10)
        metrics.increment_dead_letter()
        logger.warning(
            "Failed events sent to DLQ",
            extra={
                "dlq_topic": KAFKA_DLQ_TOPIC,
                "failed_count": len(failed_events),
            },
        )
    except Exception as exc:
        logger.error(
            "Failed to send to DLQ",
            exc_info=exc,
            extra={"failed_count": len(failed_events)},
        )


def main() -> None:
    generator = GenerateEvents()
    producer = create_producer()
    metrics = ProducerMetrics()
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
    failed_events = []
    metrics_interval = 60  # Log metrics every 60 seconds
    last_metrics_time = time.time()

    try:
        while not shutdown_requested.is_set():
            event = generator.generate_event()
            batch.append(event)

            if len(batch) >= BATCH_SIZE:
                # Send batch with retry logic
                success = send_batch_with_retry(producer, batch, metrics)
                if not success:
                    failed_events.extend(batch)
                    metrics.increment_failures()

                batch = []

                # Clean up completed futures
                futures = [f for f in futures if not f.is_done]

                # Log metrics periodically
                if time.time() - last_metrics_time > metrics_interval:
                    metrics.log_metrics()
                    last_metrics_time = time.time()

            # Small delay between generations
            shutdown_requested.wait(0.01)  # 10ms delay

        # Send remaining events in batch
        if batch:
            success = send_batch_with_retry(producer, batch, metrics)
            if not success:
                failed_events.extend(batch)

        # Send failed events to DLQ
        if failed_events:
            send_to_dlq(producer, failed_events, metrics)

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        shutdown_requested.set()
    finally:
        # Wait for all futures to complete
        for future in futures:
            try:
                future.get(timeout=10)
            except Exception as e:
                logger.error("Error waiting for future", exc_info=e)
        metrics.log_metrics()  # Final metrics
        producer.flush()
        producer.close()
        logger.info("Kafka producer closed cleanly")


if __name__ == "__main__":
    main()
