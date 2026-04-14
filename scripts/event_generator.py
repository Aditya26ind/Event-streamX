import random
import time
import uuid
from collections import defaultdict
from datetime import datetime
import json
from pathlib import Path

from app.logging_config import setup_logger
from scripts.s3_storage import S3Storage

logger = setup_logger()  # common logger setup


class GenerateEvents:

    def __init__(self, event_type=None, event_data=None):
        self.event_type = event_type
        self.event_data = event_data
        self.s3_storage = S3Storage()

    def generate_event(self):
        # Logic to generate an event based on the type and data
        logger.info("Generating event", extra={"phase": "generate_event"})
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

        logger.info("Event generated", extra={"event": event})
        return event

    def _build_partition_path(self, event, base_dir):
        event_time = datetime.fromisoformat(event["timestamp"].replace("Z", ""))
        year = event_time.strftime("%Y")
        month = event_time.strftime("%m")
        day = event_time.strftime("%d")
        return base_dir / "data" / "raw" / "events" / f"year={year}" / f"month={month}" / f"day={day}" / "events.jsonl"

    def save_event(self, events, output_path=None):
        # Logic to save the generated events (e.g., to a file or database)
        # Handle both single event and list of events for backward compatibility
        if isinstance(events, dict):
            events = [events]

        if not isinstance(events, list):
            raise TypeError("events must be a dict or list of dicts")

        event_count = len(events)
        if event_count == 0:
            resolved_path = Path(output_path) if output_path is not None else Path(
                Path(__file__).resolve().parents[1] / "data" / "raw" / "events"
            )
            return {
                "status": "no-op",
                "event_count": 0,
                "path": str(resolved_path),
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
            with output_path.open("a", encoding="utf-8") as f:
                for event in events:
                    f.write(json.dumps(event, default=str) + "\n")
            self.s3_storage.upload_file(output_path)
        else:
            base_dir = Path(__file__).resolve().parents[1]
            events_by_path = defaultdict(list)
            for event in events:
                partition_path = self._build_partition_path(event, base_dir)
                events_by_path[partition_path].append(event)

            for partition_path, partition_events in events_by_path.items():
                partition_path.parent.mkdir(parents=True, exist_ok=True)
                logger.info("Saving events",
                            extra={
                                "event_count": len(partition_events),
                                "output_path": str(partition_path)
                            })
                with partition_path.open("a", encoding="utf-8") as f:
                    for event in partition_events:
                        f.write(json.dumps(event, default=str) + "\n")
                self.s3_storage.upload_file(partition_path)

            output_path = next(iter(events_by_path))

        return {
            "status": "saved",
            "event_count": event_count,
            "path": str(output_path),
            "message": f"{event_count} events saved successfully",
        }


if __name__ == "__main__":
    generator = GenerateEvents()
    batch_size = 20
    events_batch = []

    while True:  # Run indefinitely
        event = generator.generate_event()
        events_batch.append(event)

        if len(events_batch) >= batch_size:
            generator.save_event(events_batch)
            events_batch = []  # Reset batch

        # Random interval between 1-3 seconds
        time.sleep(random.uniform(1, 3))
