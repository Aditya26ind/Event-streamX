import json
import logging
from datetime import datetime


class JsonFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "timestamp": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Include extra context if provided
        extras = {k: v for k, v in record.__dict__.items() if k not in {
            "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
            "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
            "created", "msecs", "relativeCreated", "thread", "threadName", "processName",
            "process", "message"
        }}
        if extras:
            payload["extra"] = extras

        return json.dumps(payload)


def setup_logger(name="eventstreamx", level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
    return logger
