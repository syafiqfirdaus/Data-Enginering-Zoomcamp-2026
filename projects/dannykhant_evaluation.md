# Peer Review Evaluation: dannykhant/dez-th-air-quality

**Repository:** [https://github.com/dannykhant/dez-th-air-quality](https://github.com/dannykhant/dez-th-air-quality)

## Evaluation Details

Based on the course's evaluation criteria, here is the assessment for this batch data pipeline project:

### 1. Problem Description
* **Score:** 4 points
* **Justification:** The project tackles a very well-described and highly relevant problem: tracking PM2.5 air pollution levels in Thailand during the dry 'burning season.' The objectives of historical tracking and hotspot detection via the Air4Thai API are clearly presented.

### 2. Cloud
* **Score:** 4 points
* **Justification:** Cloud usage is prominent and well-engineered. The pipeline is deployed fully on Google Cloud Platform (GCP) utilizing Google Cloud Storage and BigQuery. Furthermore, Terraform is actively utilized as an Infrastructure as Code (IaC) tool to provision these cloud resources.

### 3. Data Ingestion: Batch / Workflow orchestration
* **Criteria Evaluated:** No workflow orchestration (0 points) | Partial workflow orchestration (2 points) | End-to-end pipeline: multiple steps in the DAG, uploading data to data lake (4 points)
* **Score:** 4 points
* **Justification:** Implements robust end-to-end workflow orchestration using Apache Airflow 3. The DAG automates harvesting data from the API, processing the nested JSON payloads, loading them into the local Docker volume, and then uploading them to the GCS data lake.

### 4. Data Ingestion: Stream
* **Criteria Evaluated:** No streaming system (0 points) | Simple pipeline (2 points) | Using consumer/producers and streaming technologies (4 points)
* **Score:** 0 points
* **Justification:** Following the batch requirements of the course, this project implements a purely batch-oriented ingestion pipeline without introducing streaming technologies like Kafka or Flink.

### 5. Data Warehouse
* **Score:** 2 points
* **Justification:** Data is systematically loaded into Google BigQuery as external staging tables and then modeled downstream. However, the README does not seem to explicitly document a specific clustering or partitioning strategy designed for query optimization beyond referring to files as "partition-ready formats."

### 6. Transformations
* **Score:** 4 points
* **Justification:** Excellent usage of modern transformation tools. `dbt` is used comprehensively. The author built surrogate keys for row-level uniqueness, custom macros for AQI threshold categorization, and implemented automated data quality validation tests (`not_null`, `unique`, `accepted_range`).

### 7. Dashboard
* **Score:** 4 points
* **Justification:** Exceeds expectations. The Looker Studio dashboard provided contains three well-defined analytical tiles/charts: Top 10 Worst Provinces ranking, a Monthly PM2.5 Trend analysis, and comparative Seasonal Analysis.

### 8. Reproducibility
* **Score:** 4 points
* **Justification:** Clear instructions are distributed into logical chunks: providing terraform commands (`terraform init` / `apply`) for infrastructure, a single `docker compose up --build` command to start the entire Airflow/dbt suite locally, and leaning into Astronomer Cosmos to handle the dbt orchestrations transparently.

---

### Total Score
**26 / 32 points**

## Additional Comments
* **Data Quality & Modeling:** The inclusion of `dbt` tests for checking accepted standard ranges (0–1000 µg/m³) is a very strong and mature data engineering practice displayed here.
* **Orchestration:** Using Astronomer Cosmos within Airflow to parse dbt models into DAG tasks without requiring separate profile management is a fantastic and advanced touch.
