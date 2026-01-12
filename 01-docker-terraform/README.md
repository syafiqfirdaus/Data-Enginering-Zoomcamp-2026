# Module 1: Docker & Terraform

## Homework Solution

This directory contains the solution for Module 1 Homework.

### Answers

* **Question 1: Understanding Docker images**
  * `25.3`
* **Question 2: Understanding Docker networking and docker-compose**
  * `db:5432`
* **Question 3: Counting short trips**
  * `8,007`
* **Question 4: Longest trip for each day**
  * `2025-11-14`
* **Question 5: Biggest pickup zone**
  * `East Harlem North`
* **Question 6: Largest tip**
  * `Yorkville West`
* **Question 7: Terraform Workflow**
  * `terraform init, terraform apply -auto-approve, terraform destroy`

### How to Run

1. **Start Docker services**:

    ```bash
    docker-compose up -d
    ```

2. **Ingest data**:

    ```bash
    # Install dependencies
    pip install -r requirements.txt

    # Ingest Green Taxi Data
    python ingest_data.py --user postgres --password postgres --host localhost --port 5433 --db ny_taxi --table_name green_taxi_data --url green_tripdata_2025-11.parquet

    # Ingest Taxi Zone Lookup
    python ingest_data.py --user postgres --password postgres --host localhost --port 5433 --db ny_taxi --table_name taxi_zone_lookup --url taxi_zone_lookup.csv
    ```

3. **Check Answers**:

    ```bash
    python check_answers.py
    ```

### Docker Details

* **Postgres**: localhost:5433
* **PgAdmin**: <http://localhost:8080> (Login: `pgadmin@pgadmin.com` / `pgadmin`)
