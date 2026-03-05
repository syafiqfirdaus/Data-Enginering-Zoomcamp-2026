# Module 06 Homework - Batch Processing with Spark

## Overview

This module covers batch processing using Apache Spark (PySpark). We use the **Yellow Taxi November 2025** dataset.

**Data sources:**

- `yellow_tripdata_2025-11.parquet` — [NYC TLC](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet)
- `taxi_zone_lookup.csv` — [Taxi Zone Lookup](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv)

---

## Answers

### Q1: Install Spark and PySpark

Install PySpark, create a local session, and execute `spark.version`.

**Answer: `4.1.1`**

---

### Q2: Yellow November 2025

Read the November 2025 Yellow taxi data into a Spark DataFrame, repartition to 4 partitions, and save to Parquet.

What is the average size of the Parquet files created?

```
File sizes: [20.80 MB, 20.63 MB, 20.78 MB, 19.74 MB]
Average: ~20.5 MB
```

**Answer: `25MB`** *(closest option)*

---

### Q3: Count Records

How many taxi trips started on the 15th of November?

**Answer: `162,604`**

---

### Q4: Longest Trip

What is the length of the longest trip in the dataset in hours?

**Answer: `90.6`**

---

### Q5: User Interface

Spark's User Interface which shows the application's dashboard runs on which local port?

**Answer: `4040`**

---

### Q6: Least Frequent Pickup Location Zone

Using the zone lookup data and the Yellow November 2025 data, what is the name of the LEAST frequent pickup location Zone?

The least frequent zones (all tied at 1 trip):

- `Eltingville/Annadale/Prince's Bay`
- `Governor's Island/Ellis Island/Liberty Island`
- `Arden Heights`

**Answer: `Governor's Island/Ellis Island/Liberty Island`** *(valid answer — any of the above are correct)*

---

## Files

| File | Description |
|------|-------------|
| `hw6.py` | Python script (pandas/pyarrow) to answer all questions |
| `homework.md` | Original homework questions |
| `yellow_tripdata_2025-11.parquet` | Raw taxi data |
| `taxi_zone_lookup.csv` | Zone lookup table |
| `data/yellow_tripdata_2025-11_repartitioned/` | Repartitioned parquet files (4 parts) |
