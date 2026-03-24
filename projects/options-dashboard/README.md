# Real-Time Options Market Data Dashboard

An end-to-end streaming data pipeline project for the **Data Engineering Zoomcamp**.

## Architecture & Technologies
- **Infrastructure:** AWS EC2, Amazon Redshift Serverless, provisioned via **Terraform**
- **Data Ingestion:** Python WebSocket producer pulling real-time options trades from **Polygon.io**
- **Stream Broker:** **Redpanda** (Kafka API compatible)
- **Stream Processing:** **PyFlink** (Apache Flink) calculating rolling window VWAP (Volume-Weighted Average Price)
- **Data Warehouse:** **Amazon Redshift**
- **Dashboard:** **Streamlit**

## Setup Instructions

### 1. Provision Infrastructure
Configure your AWS CLI (`aws configure`), then run:
```bash
make tf-init
make tf-apply
```
This automatically deploys an EC2 instance, configures Redpanda Docker, injects SSH keys locally, and sets up Redshift Serverless.

### 2. Start Data Ingestion (Producer)
Ensure you have a `.env` file containing `POLYGON_API_KEY`. The Terraform step will handle the `KAFKA_BROKER` IP.
```bash
make producer-install
make producer-run
```

### 3. Start PyFlink Streaming
The Terraform deployment automatically installs Flink and dependencies on the EC2 instance. Navigate to the `terraform` directory and SSH into the machine to start the job. Flink will automatically sink the aggregated metrics mapping to Redshift.

### 4. Run the Dashboard
Finally, launch the interactive visual Streamlit dashboard:
```bash
make dashboard-install
make dashboard-run
```

## Dashboard Previews

![VWAP over Time](./vwap_over_time.png)
![Options Volume Distribution](./volume_distribution.png)
