from pathlib import Path

import pandas as pd

INPUT_FILE = Path("data/raw/events/year=2026/month=04/day=06/events.jsonl")
OUTPUT_FILE = INPUT_FILE.with_suffix(".parquet")


def convert_jsonl_to_parquet(input_path: Path, output_path: Path) -> None:
    dataframe = pd.read_json(input_path, lines=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(output_path, index=False)


def main() -> None:
    convert_jsonl_to_parquet(INPUT_FILE, OUTPUT_FILE)
    print(f"Parquet file written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
