-- Q3: Count of records in fct_monthly_zone_revenue
select count(*) as record_count 
from {{ ref('fct_monthly_zone_revenue') }};

-- Q4: Best Performing Zone for Green Taxis (2020)
-- "find the pickup zone with the highest total revenue for Green taxi trips in 2020"
select pickup_zone, sum(revenue_monthly_total_amount) as annual_revenue
from {{ ref('fct_monthly_zone_revenue') }}
where service_type = 'Green' 
  and extract(year from revenue_month) = 2020
group by pickup_zone
order by annual_revenue desc
limit 1;

-- Q5: Green Taxi Trip Counts (October 2019)
select sum(total_monthly_trips) as october_trips
from {{ ref('fct_monthly_zone_revenue') }}
where service_type = 'Green' 
  and extract(year from revenue_month) = 2019 
  and extract(month from revenue_month) = 10;

-- Q6: Count of records in stg_fhv_tripdata
select count(*) 
from {{ ref('stg_fhv_tripdata') }};
