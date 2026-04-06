

import random
import time
import uuid
from datetime import datetime
import json
from pathlib import Path

from app.logging_config import setup_logger

logger = setup_logger()  # common logger setup


class GenerateEvents:
    def __init__(self, event_type=None, event_data=None):
        self.event_type = event_type
        self.event_data = event_data

    def generate_event(self):
        # Logic to generate an event based on the type and data
        logger.info("Generating event", extra={"phase": "generate_event"})
        event_types = ["click", "login", "logout", "purchase"]
        event_type = self.event_type or random.choice(event_types)
        device_types = ["mobile", "desktop", "tablet"]
        device = random.choice(device_types)

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "user_id": f"user_{random.randint(1, 100)}",
            "timestamp": datetime.utcnow().isoformat(),
            "device": device,
            "location": random.choice(["USA", "Canada", "UK", "Germany", "France", "India", "China", "Brazil", "Australia"]),
            "amount": round(random.uniform(10.0, 500.0), 2) if event_type == "purchase" else None,
        }

        logger.info("Event generated", extra={"event": event})
        return event
    
    def save_event(self, events, output_path=None):
        # Logic to save the generated events (e.g., to a file or database)
        # Handle both single event and list of events for backward compatibility
        if isinstance(events, dict):
            events = [events]

        if not isinstance(events, list):
            raise TypeError("events must be a dict or list of dicts")

        if output_path is None:
            now = datetime.utcnow()
            year = now.strftime("%Y")
            month = now.strftime("%m")
            day = now.strftime("%d")

            base_dir = Path(__file__).resolve().parents[1]
            today = now.strftime("%Y-%m-%d")
            output_path = base_dir / "data" / "raw" / "events" / f"{year}/{month}/{day}/events_{today}.jsonl"
        else:
            output_path = Path(output_path)

        output_path.parent.mkdir(parents=True, exist_ok=True)

        event_count = len(events)
        logger.info("Saving events", extra={"event_count": event_count, "output_path": str(output_path)})

        if event_count == 0:
            return {
                "status": "no-op",
                "event_count": 0,
                "path": str(output_path),
                "message": "No events to save",
            }

        with output_path.open("a", encoding="utf-8") as f:
            for event in events:
                f.write(json.dumps(event, default=str) + "\n")

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
        