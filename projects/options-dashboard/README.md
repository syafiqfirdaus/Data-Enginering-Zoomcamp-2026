# Simulated Real-Time Options Market Data Dashboard

An end-to-end local streaming pipeline project for the Data Engineering Zoomcamp. The producer generates mock options trades; it does not consume live Polygon market data.

## Architecture & Technologies
- Data ingestion: Python producer generating simulated options trades
- Stream broker: Redpanda
- Stream processing: local Kafka consumer and aggregator
- Database: PostgreSQL
- Dashboard: Streamlit
- Local orchestration: Docker Compose

## Local Setup

### 1. Create your local env file
Copy `.env.example` to `.env`.

Recommended local values:
```env
KAFKA_BROKER=localhost:9092
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=options_dashboard
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

### 2. Start local infrastructure
From `projects/options-dashboard`:
```bash
make local-up
```

This starts:
- Redpanda on `127.0.0.1:9092`
- PostgreSQL on `127.0.0.1:5432`

Docker Compose binds the published ports to the loopback interface, so the local services are not exposed on other host network interfaces.

### 3. Install Python dependencies
```bash
make producer-install
make streaming-install
make dashboard-install
```

### 4. Create the database table
```bash
make create-table
```

### 5. Run the producer
```bash
make producer-run
```

The producer continuously generates mock options trades for demonstrating the streaming pipeline. It does not connect to Polygon or another live market-data provider.

### 6. Run the dashboard
```bash
make dashboard-run
```

## Streaming Job

The active local streaming path uses `streaming/local_aggregator.py` to read Kafka events, compute 1-minute VWAP aggregates, and write them into PostgreSQL.

## Notes

- The old AWS/Terraform deployment path has been removed from this project.
- Do not commit `.env`.
- Treat any previously exposed AWS credentials as permanently compromised.

## Dashboard Previews

![VWAP over Time](./vwap_over_time.png)
![Options Volume Distribution](./volume_distribution.png)
