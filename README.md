# 🚀 Ex-Im Bank Data Warehouse Pipeline (S3 → Airflow → MySQL)

## 📌 Overview
This project simulates a real-world **Data Engineering pipeline** for the U.S. Export-Import Bank (Ex-Im Bank).

It transforms raw quarterly export financing data into a structured data warehouse for analytics and reporting.

---

## 🏢 Business Context
The Ex-Im Bank finances international purchases of U.S. goods and services.

Each quarter, a raw CSV dataset (~51,000+ rows) is released containing:
- Approved deals
- Declined applications
- Ongoing financing activities

However, the data is:
- Unstructured
- Inconsistent (e.g. "USA" vs "United States")
- Not optimized for analytics

---

## 🎯 Objective
Build an automated ETL pipeline that:

- Extracts raw data from **AWS S3**
- Cleans and standardizes the dataset
- Loads into a **MySQL data warehouse**
- Automates execution using **Apache Airflow**
- Runs in a **Docker containerized environment**

---

## ⚙️ Tech Stack

- Python (Pandas)
- MySQL
- Apache Airflow
- Docker
- AWS S3 (boto3)
- PyMySQL

---

## 🏗️ Architecture

![Architecture Diagram](architecture.png)

---

## 🗃️ Data Warehouse Design

### Staging Layer
- `staging_export_data`

### Dimension Tables
- `dim_country`
- `dim_program`
- `dim_exporter`
- `dim_date`

### Fact Tables
- `fact_approved_deals`
- `fact_declined_deals`

---

## 🔁 Pipeline Flow

1. CSV uploaded to AWS S3
2. Airflow DAG triggers pipeline
3. Data extracted in chunks (5,000 rows)
4. Data cleaned and standardized
5. Staging table loaded
6. Dimension tables populated
7. Fact tables loaded with foreign keys
8. Data available for analytics

---

## ✅ Features

- ✔️ Chunk-based processing (handles large datasets)
- ✔️ Data cleaning and normalization
- ✔️ Star schema (fact & dimension modeling)
- ✔️ Idempotent loads (`INSERT IGNORE`)
- ✔️ Error handling and logging
- ✔️ Retry logic (Airflow-ready)
- ✔️ Fully containerized with Docker

---

## 📊 Example Queries

### Total Approved Amount
```sql
SELECT SUM(approved_amount) FROM fact_approved_deals;
