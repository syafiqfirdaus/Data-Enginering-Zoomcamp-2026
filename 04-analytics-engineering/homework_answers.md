# Module 4 Homework Answers

## Question 1: dbt Lineage and Execution

**Answer:** `int_trips_unioned` only.

## Question 2: dbt Tests

**Answer:** dbt will fail the test, returning a non-zero exit code. (Because `accepted_values` fails on new value '6').

## Question 3: Counting Records in `fct_monthly_zone_revenue`

**Query:**

```sql
SELECT count(*) 
FROM `gen-lang-client-0756788832.trips_data_all.fct_monthly_zone_revenue`;
```

**Answer:** `12,184` assigned to the closest option. (Actual calculated: `12,514` raw, `12,144` filtered for 2019-2020).

(Note: Ensure `dbt build --full-refresh` is run to include Green 2020 data. Row count includes data for 2019 and 2020.)

## Question 4: Best Performing Zone for Green Taxis (2020)

**Query:**

```sql
SELECT pickup_zone, SUM(revenue_monthly_total_amount) as annual_revenue
FROM `gen-lang-client-0756788832.trips_data_all.fct_monthly_zone_revenue`
WHERE service_type = 'Green' 
  AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY annual_revenue DESC
LIMIT 1;
```

**Answer:** `East Harlem North`

- East Harlem North: ~$2.0M
- East Harlem South: ~$1.9M
- Central Harlem: ~$1.2M

## Question 5: Green Taxi Trip Counts (October 2019)

**Query:**

```sql
SELECT SUM(total_monthly_trips) as october_trips
FROM `gen-lang-client-0756788832.trips_data_all.fct_monthly_zone_revenue`
WHERE service_type = 'Green' 
  AND EXTRACT(YEAR FROM revenue_month) = 2019 
  AND EXTRACT(MONTH FROM revenue_month) = 10;
```

**Answer:** `472,427`

(Note: requires Green 2019 data loaded)

## Question 6: FHV Staging Record Count

**Query:**

```sql
SELECT count(*) 
FROM `gen-lang-client-0756788832.trips_data_all.stg_fhv_tripdata`;
```

**Answer:** `43,261,273`

(Note: exact count depends on load completeness, ~43M is expected)
