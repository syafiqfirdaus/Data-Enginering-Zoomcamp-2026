import os
import warnings
from pathlib import Path

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st
from dotenv import load_dotenv

# Suppress noisy pandas warnings when using a raw psycopg2 connection.
warnings.filterwarnings(
    "ignore", message=".*pandas only supports SQLAlchemy connectable.*"
)

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

st.set_page_config(page_title="Simulated Real-Time Options Dashboard", layout="wide")
st.title("Simulated Real-Time Options Market Data Dashboard")
st.caption("Demonstration pipeline using generated mock trades, not live market data.")


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if value:
        return value
    raise RuntimeError(
        f"Missing required environment variable: {name}. "
        "Populate projects/options-dashboard/.env from .env.example."
    )


@st.cache_resource
def init_connection():
    conn = psycopg2.connect(
        dbname=get_required_env("POSTGRES_DATABASE"),
        user=get_required_env("POSTGRES_USER"),
        password=get_required_env("POSTGRES_PASSWORD"),
        host=get_required_env("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )
    conn.autocommit = True
    return conn


try:
    conn = init_connection()
except Exception as e:
    st.error(f"Failed to connect to PostgreSQL: {e}")
    st.stop()


@st.cache_data(ttl=2)
def fetch_data():
    query = """
    SELECT window_start, sym, vwap, total_volume
    FROM options_data
    ORDER BY window_start DESC
    LIMIT 2000
    """
    try:
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Error pulling from PostgreSQL: {str(e)}")
        return pd.DataFrame()


st.button("Refresh Data")
df = fetch_data()

if df.empty:
    st.warning("No data available yet. Ensure the stream producer and local aggregator are running!")
else:
    st.subheader("Volume-Weighted Average Price (VWAP) over Time")
    top_symbol = df["sym"].value_counts().idxmax()
    df_temporal = df[df["sym"] == top_symbol].sort_values("window_start")

    fig_temporal = px.line(
        df_temporal,
        x="window_start",
        y="vwap",
        title=f"VWAP for {top_symbol}",
        labels={"window_start": "Time", "vwap": "VWAP ($)"},
    )
    st.plotly_chart(fig_temporal, use_container_width=True)

    st.subheader("Options Volume Distribution")
    df_cat = df.groupby("sym")["total_volume"].sum().reset_index()
    fig_cat = px.bar(
        df_cat,
        x="sym",
        y="total_volume",
        title="Total Volume Traded per Option Contract",
        color="total_volume",
    )
    st.plotly_chart(fig_cat, use_container_width=True)
