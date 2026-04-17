import json
import os
import signal
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path
from threading import Event
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from kafka.structs import TopicPartition

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.logging_config import setup_logger
from scripts.event_generator import GenerateEvents

logger = setup_logger()


class Metrics:
    def __init__(self):
        self.events_processed = 0
        self.batches_processed = 0
        self.save_failures = 0
        self.dead_letter_sent = 0
        self.consumer_lag = 0
        self.throughput_events_per_sec = 0
        self.last_measurement_time = time.time()

    def increment_events(self, count: int):
        self.events_processed += count

    def increment_batches(self):
        self.batches_processed += 1

    def increment_failures(self):
        self.save_failures += 1

    def increment_dead_letter(self):
        self.dead_letter_sent += 1

    def update_lag(self, lag: int):
        self.consumer_lag = lag

    def calculate_throughput(self) -> float:
        current_time = time.time()
        elapsed = current_time - self.last_measurement_time
        if elapsed > 0:
            self.throughput_events_per_sec = self.events_processed / elapsed
        self.last_measurement_time = current_time
        return self.throughput_events_per_sec

    def log_metrics(self):
        logger.info(
            "Consumer metrics",
            extra={
                "events_processed": self.events_processed,
                "batches_processed": self.batches_processed,
                "save_failures": self.save_failures,
                "dead_letter_sent": self.dead_letter_sent,
                "consumer_lag": self.consumer_lag,
                "throughput_eps": round(self.calculate_throughput(), 2),
            },
        )


class EventConsumer:
    def __init__(
        self,
        topic: str | None = None,
        group_id: str | None = None,
        bootstrap_servers: str | None = None,
        poll_timeout_ms: int | None = None,
        batch_size: int | None = None,
        save_retries: int | None = None,
        save_retry_backoff_seconds: float | None = None,
        connect_retries: int | None = None,
        connect_retry_backoff_seconds: float | None = None,
    ) -> None:
        self.topic = topic or os.getenv("KAFKA_TOPIC", "events")
        self.group_id = group_id or os.getenv(
            "KAFKA_GROUP_ID", "eventstreamx-consumer"
        )
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.poll_timeout_ms = poll_timeout_ms or int(
            os.getenv("KAFKA_POLL_TIMEOUT_MS", "1000")
        )
        self.batch_size = batch_size or int(os.getenv("KAFKA_BATCH_SIZE", "1000"))  # Increased batch size
        self.save_retries = save_retries or int(os.getenv("KAFKA_SAVE_RETRIES", "3"))
        self.save_retry_backoff_seconds = (
            save_retry_backoff_seconds
            or float(os.getenv("KAFKA_SAVE_RETRY_BACKOFF_SECONDS", "2"))
        )
        self.connect_retries = connect_retries or int(
            os.getenv("KAFKA_CONNECT_RETRIES", "5")
        )
        self.connect_retry_backoff_seconds = (
            connect_retry_backoff_seconds
            or float(os.getenv("KAFKA_CONNECT_RETRY_BACKOFF_SECONDS", "3"))
        )

        self.dead_letter_topic = os.getenv("KAFKA_DLQ_TOPIC", "events-dlq")
        self.metrics = Metrics()
        self.shutdown_requested = Event()
        self.generator = GenerateEvents()
        self.consumer: KafkaConsumer | None = None
        self.dlq_producer: KafkaProducer | None = None
        self.batch: list[dict[str, Any]] = []
        self.batch_id = str(uuid.uuid4())  # Unique batch ID for idempotency
        self.assigned_partitions: set[TopicPartition] = set()

    def request_shutdown(self, signum, _frame) -> None:
        logger.info("Shutdown signal received", extra={"signal": signum})
        self.shutdown_requested.set()

    def create_dlq_producer(self) -> KafkaProducer:
        """Create producer for dead-letter queue."""
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else str(k).encode("utf-8"),
            acks="all",
            retries=5,
            retry_backoff_ms=1000,
        )

    def on_partitions_assigned(self, consumer, partitions):
        logger.info("Partitions assigned", extra={"partitions": [str(p) for p in partitions]})
        self.assigned_partitions = set(partitions)

    def on_partitions_revoked(self, consumer, partitions):
        logger.info("Partitions revoked, flushing batch", extra={"partitions": [str(p) for p in partitions]})
        self.flush_batch()  # Ensure batch is saved before rebalance
        self.assigned_partitions = set()

    def deserialize_message(self, raw_value: bytes) -> dict[str, Any]:
        if raw_value is None:
            raise ValueError("Kafka message value is empty")

        decoded_value = raw_value.decode("utf-8")
        event = json.loads(decoded_value)
        if not isinstance(event, dict):
            raise TypeError("Kafka message must deserialize to a JSON object")
        return event

    def create_consumer(self) -> KafkaConsumer:
        last_error = None
        for attempt in range(1, self.connect_retries + 1):
            try:
                consumer = KafkaConsumer(
                    self.topic,
                    group_id=self.group_id,
                    bootstrap_servers=self.bootstrap_servers,
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,  # Manual commit for exactly-once
                    consumer_timeout_ms=self.poll_timeout_ms,
                    max_poll_records=1000,     # Increased batch size
                    fetch_max_bytes=50 * 1024 * 1024,  # 50 MB per fetch
                    max_partition_fetch_bytes=10 * 1024 * 1024,  # 10 MB per partition
                    session_timeout_ms=30000,  # 30 seconds
                    heartbeat_interval_ms=3000,
                    max_poll_interval_ms=300000,  # 5 minutes max poll interval
                    enable_auto_commit_interval_ms=5000,  # Not used since manual
                    isolation_level="read_committed",  # For transactions if used
                )
                consumer.subscribe([self.topic], on_assign=self.on_partitions_assigned, on_revoke=self.on_partitions_revoked)
                logger.info(
                    "Kafka consumer connected",
                    extra={
                        "topic": self.topic,
                        "group_id": self.group_id,
                        "bootstrap_servers": self.bootstrap_servers,
                        "attempt": attempt,
                    },
                )
                return consumer
            except NoBrokersAvailable as exc:
                last_error = exc
                logger.warning(
                    "Kafka broker unavailable",
                    extra={
                        "topic": self.topic,
                        "group_id": self.group_id,
                        "attempt": attempt,
                        "max_attempts": self.connect_retries,
                        "retry_in_seconds": self.connect_retry_backoff_seconds,
                    },
                )
                if attempt < self.connect_retries:
                    self.shutdown_requested.wait(self.connect_retry_backoff_seconds)
            except KafkaError as exc:
                last_error = exc
                logger.exception(
                    "Kafka consumer connection failed",
                    exc_info=exc,
                    extra={
                        "attempt": attempt,
                        "max_attempts": self.connect_retries,
                    },
                )
                if attempt < self.connect_retries:
                    self.shutdown_requested.wait(self.connect_retry_backoff_seconds)

            if self.shutdown_requested.is_set():
                break

        raise RuntimeError("Unable to create Kafka consumer") from last_error

    def save_batch(self) -> dict[str, Any]:
        last_error = None
        for attempt in range(1, self.save_retries + 1):
            try:
                # Add batch metadata for idempotency
                batch_metadata = {
                    "batch_id": self.batch_id,
                    "timestamp": time.time(),
                    "event_count": len(self.batch),
                }
                result = self.generator.save_event(self.batch, batch_metadata=batch_metadata)
                logger.info(
                    "Batch persisted",
                    extra={
                        "event_count": len(self.batch),
                        "attempt": attempt,
                        "batch_id": self.batch_id,
                        "path": result.get("path"),
                    },
                )
                self.metrics.increment_events(len(self.batch))
                self.metrics.increment_batches()
                return result
            except Exception as exc:
                last_error = exc
                self.metrics.increment_failures()
                logger.exception(
                    "Failed to persist batch",
                    exc_info=exc,
                    extra={
                        "event_count": len(self.batch),
                        "attempt": attempt,
                        "max_attempts": self.save_retries,
                        "batch_id": self.batch_id,
                    },
                )
                if attempt < self.save_retries and not self.shutdown_requested.is_set():
                    self.shutdown_requested.wait(self.save_retry_backoff_seconds)

        # Send to dead-letter queue if all retries failed
        self.send_to_dlq(last_error)
        raise RuntimeError("Batch persistence failed after retries") from last_error

    def send_to_dlq(self, error: Exception) -> None:
        """Send failed batch to dead-letter queue."""
        if not self.dlq_producer:
            try:
                self.dlq_producer = self.create_dlq_producer()
            except Exception as exc:
                logger.error("Failed to create DLQ producer", exc_info=exc)
                return

        dlq_message = {
            "batch_id": self.batch_id,
            "events": self.batch,
            "error": str(error),
            "timestamp": time.time(),
            "original_topic": self.topic,
            "consumer_group": self.group_id,
        }

        try:
            future = self.dlq_producer.send(
                self.dead_letter_topic,
                key=self.batch_id,
                value=dlq_message,
            )
            future.get(timeout=10)  # Wait for send to complete
            self.metrics.increment_dead_letter()
            logger.warning(
                "Batch sent to dead-letter queue",
                extra={
                    "batch_id": self.batch_id,
                    "event_count": len(self.batch),
                    "dlq_topic": self.dead_letter_topic,
                },
            )
        except Exception as exc:
            logger.error(
                "Failed to send to dead-letter queue",
                exc_info=exc,
                extra={"batch_id": self.batch_id},
            )

    def flush_batch(self) -> None:
        if not self.batch or not self.consumer:
            return

        self.save_batch()
        self.consumer.commit()
        logger.info(
            "Offsets committed after batch save",
            extra={
                "event_count": len(self.batch),
                "group_id": self.group_id,
                "topic": self.topic,
                "batch_id": self.batch_id,
            },
        )
        self.batch = []
        self.batch_id = str(uuid.uuid4())  # New batch ID for next batch

    def process_messages(self, message_batch) -> None:
        for _topic_partition, messages in message_batch.items():
            for message in messages:
                try:
                    event = self.deserialize_message(message.value)
                    self.batch.append(event)
                    logger.info(
                        "Kafka message buffered",
                        extra={
                            "event_id": event.get("event_id"),
                            "partition": message.partition,
                            "offset": message.offset,
                            "batch_size": len(self.batch),
                        },
                    )
                except (json.JSONDecodeError, TypeError, ValueError) as exc:
                    logger.exception(
                        "Kafka message deserialization failed",
                        exc_info=exc,
                        extra={
                            "partition": message.partition,
                            "offset": message.offset,
                            "topic": message.topic,
                        },
                    )

            if len(self.batch) >= self.batch_size:
                self.flush_batch()

    def run(self) -> None:
        self.register_signal_handlers()

        try:
            self.consumer = self.create_consumer()
            self.dlq_producer = self.create_dlq_producer()
            logger.info(
                "Kafka consumer started",
                extra={
                    "topic": self.topic,
                    "group_id": self.group_id,
                    "batch_size": self.batch_size,
                    "poll_timeout_ms": self.poll_timeout_ms,
                    "dlq_topic": self.dead_letter_topic,
                },
            )

            metrics_interval = 60  # Log metrics every 60 seconds
            last_metrics_time = time.time()

            while not self.shutdown_requested.is_set():
                try:
                    message_batch = self.consumer.poll(
                        timeout_ms=self.poll_timeout_ms,
                        max_records=self.batch_size,
                    )
                except KafkaError as exc:
                    logger.exception("Kafka poll failed", exc_info=exc)
                    time.sleep(self.connect_retry_backoff_seconds)
                    continue

                if not message_batch:
                    # Log metrics periodically
                    if time.time() - last_metrics_time > metrics_interval:
                        self.metrics.log_metrics()
                        last_metrics_time = time.time()
                    continue

                self.process_messages(message_batch)

                # Update lag metrics
                if self.consumer:
                    lag = sum(
                        self.consumer.metrics().get(p, {}).get('lag', 0)
                        for p in self.assigned_partitions
                    )
                    self.metrics.update_lag(lag)

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
            self.shutdown_requested.set()
            if self.batch:
                self.flush_batch()
        finally:
            self.metrics.log_metrics()  # Final metrics
            if self.dlq_producer:
                self.dlq_producer.flush()
                self.dlq_producer.close()
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka consumer closed cleanly")


def main() -> None:
    consumer = EventConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
