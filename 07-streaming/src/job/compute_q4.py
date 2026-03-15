"""Compute answer for homework question 4 using the raw Parquet data.

This script computes which PULocationID had the most trips in any 5-minute
window (tumbling). It does not use Flink; it just applies equivalent logic
locally so we can verify the multiple-choice answer.

Run from the repo root:
    .venv/Scripts/python.exe 07-streaming/src/job/compute_q4.py
"""

import pandas as pd
from pathlib import Path


def main():
    base_dir = Path(__file__).resolve().parents[2]
    parquet_path = base_dir / "green_tripdata_2025-10.parquet"

    df = pd.read_parquet(parquet_path, columns=["lpep_pickup_datetime", "PULocationID"])
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])

    # Align to 5-minute tumbling windows by flooring the timestamp.
    df["window_start"] = df["lpep_pickup_datetime"].dt.floor("5min")

    counts = (
        df.groupby(["window_start", "PULocationID"]).size().reset_index(name="cnt")
    )

    max_row = counts.loc[counts["cnt"].idxmax()]

    print("Question 4: max trips in 5-minute window")
    print(f"  PULocationID: {int(max_row['PULocationID'])}")
    print(f"  trips: {int(max_row['cnt'])}")
    print(f"  window_start: {max_row['window_start']}")


if __name__ == "__main__":
    main()
