#!/bin/bash
cd /home/ubuntu

# Download Flink JARs
wget -q -nc https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.2/flink-sql-connector-kafka-1.17.2.jar
wget -q -nc https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.1-1.17/flink-connector-jdbc-3.1.1-1.17.jar
wget -q -nc https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Setup Python environment
sudo apt-get update
sudo apt-get install -y python3-venv libpq-dev python3-dev
python3 -m venv venv
source venv/bin/activate

# Install pyflink and database libraries
pip install -r streaming/requirements.txt
python streaming/create_table.py
python streaming/main.py
