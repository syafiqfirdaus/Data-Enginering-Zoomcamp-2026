# Module 4 Homework Answers

## Question 1: dbt Lineage and Execution

**Answer:** `stg_green_tripdata`, `stg_yellow_tripdata`, and `int_trips_unioned` (upstream dependencies)
*Correction:* The question asks what models will be built with `--select int_trips_unioned`. By default, only the selected model is built unless `+` is used.
**Revised Answer:** `int_trips_unioned` only.

## Question 2: dbt Tests

**Answer:** dbt will fail the test, returning a non-zero exit code. (Because `accepted_values` fails on new value '6').

## Question 3: Counting Records in `fct_monthly_zone_revenue`

**Query:**

```sql
select count(*) from {{ ref('fct_monthly_zone_revenue') }}
```

**Result:** Run the query after successful `dbt build`. (Expected: ~12k-15k rows)

## Question 4: Best Performing Zone for Green Taxis (2020)

**Query:**

```sql
select pickup_zone, sum(revenue_monthly_total_amount) as annual_revenue
from {{ ref('fct_monthly_zone_revenue') }}
where service_type = 'Green' 
  and extract(year from revenue_month) = 2020
group by pickup_zone
order by annual_revenue desc
limit 1;
```

**Result:** Run the query. (Likely candidates: East Harlem North, Morningside Heights)

## Question 5: Green Taxi Trip Counts (October 2019)

**Query:**

```sql
select sum(total_monthly_trips) as october_trips
from {{ ref('fct_monthly_zone_revenue') }}
where service_type = 'Green' 
  and extract(year from revenue_month) = 2019 
  and extract(month from revenue_month) = 10;
```

**Result:** Run the query.

## Question 6: Build a Staging Model for FHV Data

**Query:**

```sql
select count(*) from {{ ref('stg_fhv_tripdata') }}
```

**Result:** Run the query. (Expected: ~43M rows for full 2019 dataset)
