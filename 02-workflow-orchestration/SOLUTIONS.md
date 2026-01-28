# Module 2 Homework Solutions

## Quiz Answers

1. **Within the execution for `Yellow` Taxi data for the year `2020` and month `12`: what is the uncompressed file size?**
   - **128.3 MiB**
   *(Calculated size: 128.25 MiB)*

2. **What is the rendered value of the variable `file` when the inputs `taxi` is set to `green`, `year` is set to `2020`, and `month` is set to `04` during execution?**
   - **green_tripdata_2020-04.csv**

3. **How many rows are there for the `Yellow` Taxi data for all CSV files in the year 2020?**
   - **24,648,499**

4. **How many rows are there for the `Green` Taxi data for all CSV files in the year 2020?**
   - **1,734,051**

5. **How many rows are there for the `Yellow` Taxi data for the March 2021 CSV file?**
   - **1,925,152**

6. **How would you configure the timezone to New York in a Schedule trigger?**
   - **Add a `timezone` property set to `America/New_York` in the `Schedule` trigger configuration**

## Assignment Flows

I have created the following flows in `02-workflow-orchestration/flows/` to solve the backfill assignment:

- `10_gcp_taxi_parametrized.yaml`: A parametrized flow that accepts `taxi`, `year`, and `month` as inputs, extending the logic to start backfills for any date.
- `11_taxi_2021_backfill.yaml`: The "Challenge" flow that loops over the combinations of taxis (yellow, green) and months (2021-01 to 2021-07) to trigger the parametrized flow.

## Reproduction Script

A Python script `solve_homework.py` is included in the directory which downloads the data and verifies the row counts and file sizes.
