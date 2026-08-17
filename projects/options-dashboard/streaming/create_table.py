import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if value:
        return value
    raise RuntimeError(
        f"Missing required environment variable: {name}. "
        "Populate projects/options-dashboard/.env from .env.example."
    )


conn = psycopg2.connect(
    dbname=get_required_env("POSTGRES_DATABASE"),
    user=get_required_env("POSTGRES_USER"),
    password=get_required_env("POSTGRES_PASSWORD"),
    host=get_required_env("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT", "5432"),
)
conn.autocommit = True
cur = conn.cursor()

cur.execute(
    """
CREATE TABLE IF NOT EXISTS options_data (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    sym VARCHAR,
    vwap DOUBLE PRECISION,
    total_volume BIGINT
);
"""
)
cur.execute(
    """
CREATE UNIQUE INDEX IF NOT EXISTS options_data_window_symbol_idx
ON options_data (window_start, sym);
"""
)
print("PostgreSQL table 'options_data' created successfully!")
