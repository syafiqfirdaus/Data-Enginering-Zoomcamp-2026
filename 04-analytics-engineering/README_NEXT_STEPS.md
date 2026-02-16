# Project Completion Status

## Delivered

- **dbt Project:** Fully configured in `04-analytics-engineering/taxi_rides_ny`.
- **Models:** Staging (Green, Yellow, FHV), Core (Dim Zones, Fact Trips), Marts (Monthly Revenue).
- **Scripts:** Data loading scripts in `04-analytics-engineering/scripts`.
- **Answers:** SQL queries documented in `homework_queries.sql` and `homework_answers.md`.

## Known Issues

- **Data Loading:** Network restrictions in the current environment prevented full download of Green and FHV taxi data.
  - **Yellow Taxi Data:** Partially loaded (22M rows).
  - **Green/FHV Data:** Empty tables created to allow dbt compilation.

## Next Steps

To complete the homework requires running the data load in an environment with unrestricted internet access:

1. Run `python 04-analytics-engineering/scripts/download_and_load_local.py` to download missing data.
2. Run `dbt build --target prod` to populate all models.
3. Execute queries in `homework_answers.md` to get final numbers.
