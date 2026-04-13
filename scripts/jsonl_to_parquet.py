from pathlib import Path
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.logging_config import setup_logger

RAW_EVENTS_DIR = Path("data/raw/events")
PROCESSED_EVENTS_DIR = Path("data/processed/events")
logger = setup_logger()


def convert_jsonl_to_parquet(input_path: Path, output_path: Path) -> None:
    dataframe = pd.read_json(input_path, lines=True)
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
    
    dataframe=dataframe[["event_id", "event_type", "user_id", "event_timestamp", "device", "location", "amount"]]
    
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(output_path, index=False)


def main() -> None:
    input_files = sorted(RAW_EVENTS_DIR.rglob("events.jsonl"))
    logger.info(
        "Found JSONL files to process",
        extra={"file_count": len(input_files), "source_dir": str(RAW_EVENTS_DIR)},
    )

    for input_file in input_files:
        splitted = input_file.parts
        if len(splitted) < 5:
            logger.warning(
                "Skipping file with unexpected path structure",
                extra={"input_path": str(input_file)},
            )
            continue
        else:
            year = [p for p in splitted if "year=" in p][0]
            month = [p for p in splitted if "month=" in p][0]
            day = [p for p in splitted if "day=" in p][0]

        output_file = PROCESSED_EVENTS_DIR / year / month / day / "events.parquet"
        if output_file.exists():
            logger.info(
                "Output Parquet file already exists, skipping conversion",
                extra={"output_path": str(output_file)},
            )
            continue

        logger.info(
            "Converting JSONL to Parquet",
            extra={"input_path": str(input_file), "output_path": str(output_file)},
        )
        convert_jsonl_to_parquet(input_file, output_file)
        logger.info("Parquet file written", extra={"output_path": str(output_file)})


if __name__ == "__main__":
    main()
