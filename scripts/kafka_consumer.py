import json
import os
import signal
import sys
import time
from pathlib import Path
from threading import Event
from typing import Any

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.logging_config import setup_logger
from scripts.event_generator import GenerateEvents

logger = setup_logger()


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
        self.batch_size = batch_size or int(os.getenv("KAFKA_BATCH_SIZE", "100"))
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

        self.shutdown_requested = Event()
        self.generator = GenerateEvents()
        self.consumer: KafkaConsumer | None = None
        self.batch: list[dict[str, Any]] = []

    def request_shutdown(self, signum, _frame) -> None:
        logger.info("Shutdown signal received", extra={"signal": signum})
        self.shutdown_requested.set()

    def register_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

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
                    enable_auto_commit=False,
                    consumer_timeout_ms=self.poll_timeout_ms,
                )
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
                result = self.generator.save_event(self.batch)
                logger.info(
                    "Batch persisted",
                    extra={
                        "event_count": len(self.batch),
                        "attempt": attempt,
                        "path": result.get("path"),
                    },
                )
                return result
            except Exception as exc:
                last_error = exc
                logger.exception(
                    "Failed to persist batch",
                    exc_info=exc,
                    extra={
                        "event_count": len(self.batch),
                        "attempt": attempt,
                        "max_attempts": self.save_retries,
                    },
                )
                if attempt < self.save_retries and not self.shutdown_requested.is_set():
                    self.shutdown_requested.wait(self.save_retry_backoff_seconds)

        raise RuntimeError("Batch persistence failed after retries") from last_error

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
            },
        )
        self.batch = []

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
            logger.info(
                "Kafka consumer started",
                extra={
                    "topic": self.topic,
                    "group_id": self.group_id,
                    "batch_size": self.batch_size,
                    "poll_timeout_ms": self.poll_timeout_ms,
                },
            )

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
                    continue

                self.process_messages(message_batch)

            if self.batch:
                logger.info(
                    "Shutdown requested, flushing final batch",
                    extra={"event_count": len(self.batch)},
                )
                self.flush_batch()
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
            self.shutdown_requested.set()
            if self.batch:
                self.flush_batch()
        finally:
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka consumer closed cleanly")


def main() -> None:
    consumer = EventConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
