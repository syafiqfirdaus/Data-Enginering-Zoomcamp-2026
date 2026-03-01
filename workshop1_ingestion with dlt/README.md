# dlt Pipeline Ingestion Workshop

## Overview

This workshop demonstrates building a production-ready dlt pipeline that ingests NYC Yellow Taxi data from a custom REST API with pagination support. The pipeline loads data into DuckDB and includes helper scripts to compute dataset statistics and answer analysis questions.

## Project Structure

```
workshop1_ingestion with dlt/
├── taxi_pipeline.py           # dlt source with paginated REST API integration
├── query_taxi.py              # Helper script for data analysis
├── inspect_db.py              # Database inspection utility
├── taxi_pipeline.duckdb       # DuckDB database (auto-generated)
├── dlt_homework.md            # Original homework assignment
└── README.md                  # This file
```

## Key Files

### `taxi_pipeline.py`
- Defines a `@dlt.source` that fetches taxi records from the custom API
- Handles pagination automatically (stops when empty page is returned)
- Loads data into DuckDB with dataset name `taxi_pipeline_dataset`
- Includes a `run_queries()` helper to display results

**Usage:**
```bash
python taxi_pipeline.py
```

**Output:**
```
starting taxi_pipeline script
Pipeline taxi_pipeline load step completed in 2.53 seconds
1 load package(s) were loaded to destination duckdb and into dataset taxi_pipeline_dataset
...
dataset start 2009-06-01 19:33:00+08:00 end 2009-07-01 07:58:00+08:00
credit card share (%) 26.66
total tip_amount 6063.410000000009
```

### `query_taxi.py`
Standalone script for analyzing the loaded data. Connects to the DuckDB database and runs three analysis queries:

**Usage:**
```bash
python query_taxi.py
```

**Output:**
```
date range [(datetime.datetime(2009, 6, 1 ...), datetime.datetime(2009, 7, 1 ...))]
credit pct [(26.66,)]
tip total [(6063.41,)]
```

### `inspect_db.py`
Debug utility to inspect the DuckDB schema and table structure.

## Implementation Details

### Data Source
- **Base URL:** `https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api`
- **Format:** Paginated JSON responses
- **Page Size:** ~1,000 records per page
- **Pagination:** Starts at page 1; stops when empty page returned

### Pipeline Configuration
- **Destination:** DuckDB (local file)
- **Dataset Schema:** `taxi_pipeline_dataset`
- **Table Name:** `trips`
- **Columns:** 20+ fields including pickup/dropoff times, fare amounts, payment type, tip amount, etc.

### Column Mapping
The API returns columns with PascalCase names (e.g., `Fare_Amt`), which are automatically normalized by dlt to snake_case in DuckDB (e.g., `fare_amt`).

**Key columns for homework:**
- `trip_pickup_date_time` – Trip start timestamp
- `payment_type` – Payment method (Credit, CASH, etc.)
- `tip_amt` – Tip amount in dollars

## Homework Answers

### Question 1: Dataset Date Range
**Answer:** 2009-06-01 to 2009-07-01

**Query:**
```sql
SELECT MIN(trip_pickup_date_time), MAX(trip_pickup_date_time) FROM trips
```

### Question 2: Credit Card Payment Proportion
**Answer:** 26.66%

**Query:**
```sql
SELECT 100.0 * SUM(payment_type='Credit')/COUNT(*) FROM trips
```

### Question 3: Total Tips Generated
**Answer:** $6,063.41

**Query:**
```sql
SELECT SUM(tip_amt) FROM trips
```

## Running the Pipeline

### Prerequisites
- Python 3.8+
- dlt with DuckDB support: `pip install "dlt[workspace]"`

### Step 1: Initialize dlt Project
```bash
dlt init dlthub:taxi_pipeline duckdb
```

### Step 2: Run the Pipeline
```bash
python taxi_pipeline.py
```

This will:
1. Fetch all pages from the custom API
2. Load records into DuckDB
3. Run the analysis queries and print results

### Step 3: Verify Results
```bash
python query_taxi.py
```

## Database Schema

After running the pipeline, the DuckDB database contains:

**Schema:** `taxi_pipeline_dataset`

**Tables:**
- `trips` – Main data table with ~10,000 taxi records
- `_dlt_loads` – Pipeline load metadata
- `_dlt_pipeline_state` – Pipeline state tracking
- `_dlt_version` – dlt version info

**Sample Query:**
```bash
python -c "import duckdb; conn = duckdb.connect('taxi_pipeline.duckdb'); print(conn.sql('SELECT COUNT(*) FROM taxi_pipeline_dataset.trips').fetchall())"
```

## Learning Outcomes

This workshop covers:
- ✅ Building custom REST API sources with dlt
- ✅ Implementing pagination in data pipelines
- ✅ Loading data into DuckDB
- ✅ SQL analysis on ingested data
- ✅ Error handling and debugging
- ✅ Pipeline metadata inspection

## Troubleshooting

**Table not found error:**
```
CatalogException: Table with name trips does not exist!
```
→ Ensure you're using the correct schema: `SET search_path='taxi_pipeline_dataset'`

**Connection timeout:**
→ The custom API may be rate-limited or temporarily unavailable. Wait and retry.

**Empty pages:**
→ The pipeline will stop fetching when it receives an empty JSON array (expected behavior).

## Resources

- [dlt Documentation](https://dlthub.com/docs)
- [DuckDB Guide](https://duckdb.org/docs)
- [dlt REST API Tutorial](https://dlthub.com/docs/build-a-pipeline/source)

## Next Steps

Possible extensions:
- Add incremental loading based on trip date
- Integrate with dbt for data transformation
- Build visualizations with marimo or Looker Studio
- Add data validation and quality checks
- Deploy pipeline to cloud (GCP, AWS, Azure)

