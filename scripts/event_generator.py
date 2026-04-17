import random
import time
import uuid
from collections import defaultdict
from datetime import datetime
import json
import os
from pathlib import Path

from app.logging_config import setup_logger
from scripts.s3_storage import S3Storage

logger = setup_logger()  # common logger setup


class GenerateEvents:

    def __init__(self, event_type=None, event_data=None):
        self.event_type = event_type
        self.event_data = event_data
        self.s3_storage = S3Storage()
        self.production_mode = os.getenv("PRODUCTION_MODE", "false").lower() == "true"

    def generate_event(self):
        # Logic to generate an event based on the type and data
        event_type = self.event_type or random.choices(
            ["click", "login", "logout", "purchase"],
            weights=[50, 20, 10, 20],
            k=1)[0]
        device_types = ["mobile", "desktop", "tablet"]
        device = random.choice(device_types)

        event = {
            "event_id":
            str(uuid.uuid4()),
            "event_type":
            event_type,
            "user_id":
            f"user_{random.randint(1, 100)}",
            "timestamp":
            datetime.utcnow().isoformat(),
            "device":
            device,
            "location":
            random.choice([
                "USA", "Canada", "UK", "Germany", "France", "India", "China",
                "Brazil", "Australia"
            ]),
            "amount":
            round(random.uniform(10.0, 500.0), 2)
            if event_type == "purchase" else None,
        }

        return event

    def _build_partition_path(self, event, base_dir):
        event_time = datetime.fromisoformat(event["timestamp"].replace("Z", ""))
        year = event_time.strftime("%Y")
        month = event_time.strftime("%m")
        day = event_time.strftime("%d")
        return base_dir / "data" / "raw" / "events" / f"year={year}" / f"month={month}" / f"day={day}" / "events.jsonl"

    def save_event(self, events, output_path=None, batch_metadata=None):
        # Logic to save the generated events (e.g., to a file or database)
        # Handle both single event and list of events for backward compatibility
        if isinstance(events, dict):
            events = [events]

        if not isinstance(events, list):
            raise TypeError("events must be a dict or list of dicts")

        batch_id = batch_metadata.get("batch_id", str(uuid.uuid4())) if batch_metadata else str(uuid.uuid4())
        event_count = len(events)
        if event_count == 0:
            resolved_path = Path(output_path) if output_path is not None else Path(
                Path(__file__).resolve().parents[1] / "data" / "raw" / "events"
            )
            return {
                "status": "no-op",
                "event_count": 0,
                "path": str(resolved_path),
                "batch_id": batch_id,
                "message": "No events to save",
            }

        if output_path is not None:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            logger.info("Saving events",
                        extra={
                            "event_count": event_count,
                            "output_path": str(output_path)
                        })
            self._persist_events(events, output_path)
        else:
            base_dir = Path(__file__).resolve().parents[1]
            events_by_path = defaultdict(list)
            for event in events:
                partition_path = self._build_partition_path(event, base_dir, batch_id)
                events_by_path[partition_path].append(event)

            for partition_path, partition_events in events_by_path.items():
                partition_path.parent.mkdir(parents=True, exist_ok=True)
                logger.info("Saving events",
                            extra={
                                "event_count": len(partition_events),
                                "output_path": str(partition_path)
                            })
                self._persist_events(partition_events, partition_path)

            output_path = next(iter(events_by_path))

        return {
            "status": "saved",
            "event_count": event_count,
            "path": str(output_path),
            "batch_id": batch_id,
            "message": f"{event_count} events saved successfully",
        }

    def _persist_events(self, events, output_path: Path) -> None:
        payload = "".join(json.dumps(event, default=str) + "\n" for event in events)

        if not self.production_mode:
            with output_path.open("a", encoding="utf-8") as f:
                f.write(payload)
            self.s3_storage.upload_file(output_path)
            return

        if self.s3_storage.enabled:
            object_key = self.s3_storage.object_key_from_path(output_path)
            upload_succeeded = self.s3_storage.upload_bytes(
                payload.encode("utf-8"),
                object_key,
            )
            if not upload_succeeded:
                raise RuntimeError(f"Failed to write events to S3: {object_key}")
            return

        raise RuntimeError("Production mode requires S3_STORAGE_ENABLED=true")

    def _build_partition_path(self, event: dict, base_dir: Path, batch_id: str = None) -> Path:
        """Build partitioned path for event with unique batch naming."""
        timestamp = datetime.fromisoformat(event["timestamp"])
        year = timestamp.strftime("%Y")
        month = timestamp.strftime("%m")
        day = timestamp.strftime("%d")

        # Use batch_id for unique file naming to ensure idempotency
        batch_suffix = f"_{batch_id}" if batch_id else ""
        filename = f"events{batch_suffix}.jsonl"

        return (
            base_dir
            / "data"
            / "raw"
            / "events"
            / f"year={year}"
            / f"month={month}"
            / f"day={day}"
            / filename
        )


if __name__ == "__main__":
    generator = GenerateEvents()
    batch_size = 20
    events_batch = []

    while True:  # Run indefinitely
        event = generator.generate_event()
        events_batch.append(event)

        if len(events_batch) >= batch_size:
            logger.info("Generated batch of events", extra={"batch_size": len(events_batch)})
            generator.save_event(events_batch)
            events_batch = []  # Reset batch
