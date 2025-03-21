# Data Engineering Demo Project

This is an end-to-end Data Engineering demonstration project showcasing modern tools and best practices, including data generation, orchestration, ETL processes, cloud infrastructure, CI/CD, and data visualization.

---

## 🚀 Project Overview

The project simulates data ingestion and analytics for a fictional retail store, generating customer data, performing ETL processes, and visualizing insights.

### 🛠️ Tech Stack
- **Data Generation**: Python, Faker, Docker, cron
- **Data Storage and ETL**: JSON/CSV, Google Cloud Storage (GCS), BigQuery, PostgreSQL
- **Orchestration**: Apache Airflow
- **Cloud Infrastructure**: Google Cloud Platform, Terraform
- **CI/CD**: GitHub Actions
- **Monitoring and Logging**: Airflow logs and monitoring tools
- **Data Visualization**: TBD (Looker Studio, Metabase, Superset)

---

## 📌 Project Workflow

### 1. **Data Generation (Docker, Faker, cron)**
- Docker container generates synthetic customer data using Python and Faker.
- Cron runs hourly from 10:00 to 20:00, creating 300-500 customer records per day.
- Generated data simulates realistic purchasing patterns based on time.
- Data stored as JSON or CSV files.

### 2. **Upload Data to Google Cloud Storage (Airflow)**
- Daily Airflow DAG uploads raw data files from Docker to GCS at midnight.
- Files are deleted locally after successful upload.

### 3. **Import Data into BigQuery (Airflow)**
- Airflow DAG imports files from GCS into BigQuery.
- Automatic table creation and data quality checks (duplicates, null values).

### 4. **Data Aggregation and Export to PostgreSQL (Airflow)**
- Aggregation queries executed in BigQuery (e.g., average age, top professions, peak shopping hours).
- Aggregated data exported to PostgreSQL database for analytical purposes.

### 5. **Visualization and Analytics**
- Connect a BI tool (e.g., Looker Studio, Metabase) to PostgreSQL.
- Create dashboards and visualizations for actionable insights.

---

## 🚦 CI/CD Pipeline (GitHub Actions)

Automated CI/CD workflows include:
- Code linting and testing (`pytest`, `flake8`)
- Terraform infrastructure deployment
- Docker image build and deployment
- Airflow DAG deployment

---

## ✅ Additional Features

- **Monitoring & Logging**: Comprehensive monitoring and logging via Airflow.
- **Data Quality Checks**: Ensuring data integrity through automated validation checks.
- **Secrets Management**: Securely managing credentials via GCP Secret Manager.

---

## 📝 Setup Instructions

Detailed setup instructions, infrastructure deployment guidelines, and usage examples will be provided here upon project implementation.

---

## 📋 Future Enhancements
- Integration of advanced analytics and machine learning.
- Expanded monitoring and alerting systems.

