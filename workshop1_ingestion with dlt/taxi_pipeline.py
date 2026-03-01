print('starting taxi_pipeline script')
import dlt
import requests

BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"

@dlt.source

def taxi_source():
    """dlt source that fetches paginated taxi records from the custom API."""
    @dlt.resource(name="trips")
    def trips():
        page = 1
        while True:
            resp = requests.get(BASE_URL, params={"page": page})
            resp.raise_for_status()
            data = resp.json()
            if not data:  # empty page terminates pagination
                break
            for row in data:
                yield row
            page += 1
    # return the resource function (the decorator adds name)
    return trips


def run_queries():
    """Helper that opens the DuckDB file and runs the three homework queries."""
    import duckdb

    conn = duckdb.connect("taxi_pipeline.duckdb")
    # the dataset schema is created by dlt
    conn.sql("SET search_path='taxi_pipeline_dataset'")

    date_range = conn.sql(
        "SELECT MIN(trip_pickup_date_time), MAX(trip_pickup_date_time) FROM trips"
    ).fetchone()
    credit_pct = conn.sql(
        "SELECT 100.0 * SUM(payment_type='Credit')/COUNT(*) FROM trips"
    ).fetchone()[0]
    total_tips = conn.sql("SELECT SUM(tip_amt) FROM trips").fetchone()[0]

    print("dataset start", date_range[0], "end", date_range[1])
    print("credit card share (%)", credit_pct)
    print("total tip_amount", total_tips)


if __name__ == "__main__":
    # run pipeline
    pipeline = dlt.pipeline("taxi_pipeline", destination="duckdb")
    info = pipeline.run(taxi_source())
    print(info)
    # run the simple queries so we can immediately see answers
    run_queries()
