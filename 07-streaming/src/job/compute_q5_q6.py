"""Compute answers for homework questions 5 and 6 from the raw Parquet data.

This script does not require running Flink; it runs locally against the parquet
file and prints the values the Flink jobs are expected to compute.

Run from the repo root:
    .venv/Scripts/python.exe 07-streaming/src/job/compute_q5_q6.py
"""

from pathlib import Path

import pandas as pd


def main():
    # Use the data file located in the 07-streaming folder (relative to this script)
    base_dir = Path(__file__).resolve().parents[2]
    parquet_path = base_dir / "green_tripdata_2025-10.parquet"

    # Load minimal columns to compute the answers
    df = pd.read_parquet(parquet_path, columns=[
        "lpep_pickup_datetime",
        "PULocationID",
        "tip_amount",
    ])

    # Ensure datetime column is parsed
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])

    # Question 6: total tip per hour
    hourly = (
        df.set_index("lpep_pickup_datetime")["tip_amount"]
        .resample("1h")
        .sum()
        .reset_index()
    )
    max_tip = hourly.loc[hourly["tip_amount"].idxmax()]

    # Question 5: session window with 5-minute gap per PULocationID
    df = df.sort_values(["PULocationID", "lpep_pickup_datetime"])
    df["prev_ts"] = df.groupby("PULocationID")["lpep_pickup_datetime"].shift(1)
    df["gap_min"] = (df["lpep_pickup_datetime"] - df["prev_ts"]).dt.total_seconds() / 60
    df["new_session"] = df["gap_min"].isna() | (df["gap_min"] > 5)
    df["session_id"] = df.groupby("PULocationID")["new_session"].cumsum()

    session_counts = (
        df.groupby(["PULocationID", "session_id"]).size().reset_index(name="trips")
    )
    max_session = session_counts.loc[session_counts["trips"].idxmax()]

    print("Question 5: longest session")
    print(f"  PULocationID: {int(max_session['PULocationID'])}")
    print(f"  trips in session: {int(max_session['trips'])}")

    print("  top 5 session sizes:")
    print(session_counts["trips"].value_counts().nlargest(5).to_string())

    print("\nQuestion 6: hour with highest total tip")
    print(f"  hour start: {max_tip['lpep_pickup_datetime']}")
    print(f"  total tip: {max_tip['tip_amount']}")


if __name__ == "__main__":
    main()
