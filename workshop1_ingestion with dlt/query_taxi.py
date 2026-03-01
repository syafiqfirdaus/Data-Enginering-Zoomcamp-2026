import duckdb

def query(sql):
    conn = duckdb.connect("taxi_pipeline.duckdb")
    # use the dataset schema created by the pipeline
    conn.sql("SET search_path='taxi_pipeline_dataset'")
    return conn.sql(sql).fetchall()

if __name__ == "__main__":
    print("date range", query("SELECT MIN(trip_pickup_date_time), MAX(trip_pickup_date_time) FROM trips"))
    print("credit pct", query("SELECT 100.0 * SUM(payment_type='Credit')/COUNT(*) FROM trips"))
    print("tip total", query("SELECT SUM(tip_amt) FROM trips"))
