"""Main entry point for EventStreamX event generator."""

import json
import sys
from datetime import datetime
from pathlib import Path
from time import sleep

# ensure root project path is on import search path
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.logging_config import setup_logger

logger = setup_logger()

from scripts.event_generator import GenerateEvents


def main():
    """Run event generation continuously with a fixed interval."""
    startup_payload = {
        "status": "starting",
        "service": "eventstreamx",
        "message": "EventStreamX generator starting",
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    print(json.dumps(startup_payload))
    logger.info("Startup payload emitted", extra={"payload": startup_payload})

    generator = GenerateEvents()

    # Optional initial startup delay
    sleep(4)

    interval_seconds = 4
    while True:
        try:
            event = generator.generate_event()
            save_result = generator.save_event(event)
            success_payload = {
                "status": "ok",
                "message": "Event processed",
                "event_id": event.get("event_id"),
                "driver": save_result,
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }
            print(json.dumps(success_payload))
            logger.info("Event processed", extra={"payload": success_payload})
        except Exception as exc:
            error_payload = {
                "status": "error",
                "message": "Event stream failure",
                "error": str(exc),
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }
            print(json.dumps(error_payload))
            logger.exception("Error in event loop", exc_info=exc, extra={"payload": error_payload})

        sleep(interval_seconds)


if __name__ == "__main__":
    main()
