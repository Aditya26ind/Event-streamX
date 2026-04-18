from pathlib import Path
from io import BytesIO
import os
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.logging_config import setup_logger
from scripts.s3_storage import S3Storage

RAW_EVENTS_DIR = Path("data/raw/events")
PROCESSED_EVENTS_DIR = Path("data/processed/events")
logger = setup_logger()
s3_storage = S3Storage()
PRODUCTION_MODE = os.getenv("PRODUCTION_MODE", "false").lower() == "true"


def _transform_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe=dataframe.astype(
        {
            "event_id": "string",
            "event_type": "string",
            "device": "string",
            "location": "string",
            "amount": "float64"
        }
        
    )
    
    #cleaning
    dataframe.dropna(subset=["event_id", "event_type", "timestamp"], inplace=True)
    dataframe["event_id"] = dataframe["event_id"].str.strip()
    dataframe["user_id"] = dataframe["user_id"].str.strip()
    dataframe["event_type"] = dataframe["event_type"].str.strip()
    dataframe["device"] = dataframe["device"].str.strip()
    dataframe["location"] = dataframe["location"].str.strip()
    dataframe["event_timestamp"] = pd.to_datetime(dataframe["timestamp"], errors="coerce")
    dataframe.drop(columns=["timestamp"], inplace=True)
    dataframe.loc[dataframe["event_type"] != "purchase", "amount"] = None
    
    return dataframe[["event_id", "event_type", "user_id", "event_timestamp", "device", "location", "amount"]]


def convert_jsonl_to_parquet(input_key: str, output_key: str) -> None:
    raw_payload = s3_storage.read_file(input_key)
    dataframe = pd.read_json(BytesIO(raw_payload), lines=True)
    dataframe = _transform_dataframe(dataframe)

    parquet_buffer = BytesIO()
    dataframe.to_parquet(parquet_buffer, index=False)
    upload_succeeded = s3_storage.upload_bytes(parquet_buffer.getvalue(), output_key)
    if not upload_succeeded:
        raise RuntimeError(f"Failed to write Parquet to S3: {output_key}")


def convert_local_jsonl_to_parquet(input_path: Path, output_path: Path) -> None:
    dataframe = pd.read_json(input_path, lines=True)
    dataframe = _transform_dataframe(dataframe)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(output_path, index=False)
    s3_storage.upload_file(output_path)


def build_processed_key(input_key: str) -> str:
    splitted = Path(input_key).parts
    if len(splitted) < 5:
        raise ValueError(f"Unexpected S3 key structure: {input_key}")

    year = [p for p in splitted if "year=" in p][0]
    month = [p for p in splitted if "month=" in p][0]
    day = [p for p in splitted if "day=" in p][0]
    return (PROCESSED_EVENTS_DIR / year / month / day / "events.parquet").as_posix()


def main() -> None:
    if PRODUCTION_MODE:
        run_s3_pipeline()
        return

    run_local_pipeline()


def run_s3_pipeline() -> None:
    if not s3_storage.enabled:
        raise RuntimeError("Production mode requires S3_STORAGE_ENABLED=true")

    input_files = sorted(
        key for key in s3_storage.list_keys(RAW_EVENTS_DIR.as_posix()) if key.endswith("events.jsonl")
    )
    logger.info(
        "Found JSONL files to process from S3",
        extra={"file_count": len(input_files), "source_prefix": str(RAW_EVENTS_DIR)},
    )

    existing_outputs = set(s3_storage.list_keys(PROCESSED_EVENTS_DIR.as_posix()))

    for input_file in input_files:
        try:
            output_file = build_processed_key(input_file)
        except ValueError:
            logger.warning(
                "Skipping file with unexpected S3 key structure",
                extra={"input_key": input_file},
            )
            continue

        if output_file in existing_outputs:
            logger.info(
                "Output Parquet object already exists in S3, skipping conversion",
                extra={"output_key": output_file},
            )
            continue

        logger.info(
            "Converting JSONL to Parquet from S3",
            extra={"input_key": input_file, "output_key": output_file},
        )
        convert_jsonl_to_parquet(input_file, output_file)
        logger.info("Parquet file written to S3", extra={"output_key": output_file})


def run_local_pipeline() -> None:
    input_files = sorted(RAW_EVENTS_DIR.rglob("events.jsonl"))
    logger.info(
        "Found JSONL files to process from local storage",
        extra={"file_count": len(input_files), "source_dir": str(RAW_EVENTS_DIR)},
    )

    for input_file in input_files:
        splitted = input_file.parts
        
        # Check if file has the expected partitioned structure
        year_parts = [p for p in splitted if "year=" in p]
        month_parts = [p for p in splitted if "month=" in p]
        day_parts = [p for p in splitted if "day=" in p]
        
        if not year_parts or not month_parts or not day_parts:
            logger.warning(
                "Skipping file with unexpected path structure (missing year/month/day partitions)",
                extra={"input_path": str(input_file)},
            )
            continue

        year = year_parts[0]
        month = month_parts[0]
        day = day_parts[0]

        output_file = PROCESSED_EVENTS_DIR / year / month / day / "events.parquet"
        if output_file.exists():
            logger.info(
                "Output Parquet file already exists, skipping conversion",
                extra={"output_path": str(output_file)},
            )
            continue

        logger.info(
            "Converting JSONL to Parquet from local storage",
            extra={"input_path": str(input_file), "output_path": str(output_file)},
        )
        convert_local_jsonl_to_parquet(input_file, output_file)
        logger.info("Parquet file written", extra={"output_path": str(output_file)})


if __name__ == "__main__":
    main()
