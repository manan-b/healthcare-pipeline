# 🏥 Healthcare Patient Analytics Pipeline

## 📖 Project Overview
This project is an end-to-end Healthcare Patient Analytics Pipeline built using Databricks, PySpark, Delta Lake, AWS S3, and Python. The pipeline ingests raw patient clinical and demographic records, applies complex data transformations and quality checks, and produces business-ready analytics tables. It implements a robust **Medallion Architecture (Bronze ➔ Silver ➔ Gold)** to ensure data quality, traceability, and scalable performance, enabling healthcare providers to analyze patient risks, hospital performance, and diagnosis trends.

## 🏗️ Architecture Overview (Medallion Architecture)
The data pipeline follows the best practices of the Databricks Medallion Architecture:
- **🥉 Bronze Layer (Raw):** Raw data is ingested from an AWS Glue Data Catalog and stored in an immutable, append-only Delta table. Metadata columns (ingestion timestamp, source file, batch ID) are appended to guarantee full lineage and traceability.
- **🥈 Silver Layer (Cleaned & Conformed):** Data is read incrementally, cleaned, and validated. Transformations include gender and smoking status standardization, categorical null-filling, clinical range validation (e.g., valid blood pressure or BMI ranges), and patient-level deduplication using `MERGE` (Upsert) operations.
- **🥇 Gold Layer (Curated Business-Level Tables):** Aggregated metrics and KPIs are computed into specialized Delta tables for BI dashboards and downstream analytics. These include patient risk scores, hospital-level statistics, diagnosis prevalence, and a daily KPI summary.

## 📂 Data Source Description
The pipeline processes continuous healthcare streams and batch records encompassing patient demographics, vitals, lab results, and hospital information. The primary data source ingested is `patient_records.csv`, which contains over 56,000+ patient records (approx. 8.3 MB of raw healthcare data) structured with cardiovascular risk factors, lifestyle choices, and clinical diagnoses.

## 📊 Dataset Fields / Schema Overview
The primary dataset includes 32 critical features. Key fields include:
- **Demographics & ID:** `patient_id`, `age`, `gender`, `hospital_id`, `doctor_id`
- **Vitals & Measurements:** `height_cm`, `weight_kg`, `bmi`, `systolic_bp`, `diastolic_bp`, `heart_rate`, `max_heart_rate`
- **Lab Results:** `cholesterol_total`, `hdl_cholesterol`, `ldl_cholesterol`, `triglycerides`, `fasting_glucose`, `hba1c`
- **Lifestyle Factors:** `smoking_status`, `alcohol_consumption`, `physical_activity_level`
- **Clinical History & Diagnosis:** `family_history_heart`, `diabetes`, `hypertension`, `chest_pain_type`, `resting_ecg`, `exercise_induced_angina`, `diagnosis_code`, `diagnosis_description`, `heart_disease`

## ⚙️ Pipeline Workflow
1. **Source Storage:** Raw CSV files are uploaded to an AWS S3 bucket.
2. **Cataloging:** AWS Glue Crawlers automatically infer the schema from S3 and register it in the Glue Data Catalog.
3. **Bronze Ingestion:** A Databricks Spark job reads the Glue-cataloged table, appends audit metadata, performs initial type casting, and appends the data partitioned by `_ingestion_date` into the Bronze Delta table.
4. **Silver Transformation:** An incremental PySpark job reads only the newly ingested Bronze data, applies data quality rules, and performs an Upsert (`MERGE INTO`) into the Silver Delta table, partitioned by `diagnosis_code`.
5. **Gold Analytics:** Cleaned Silver data is aggregated into four distinct Gold Delta tables, ready to be served to reporting layers.
6. **Logging:** A custom logging framework writes execution metadata to a distinct `pipeline_log` table.

## 🥉 Bronze Layer Description
The Bronze layer serves as the historical archive of all ingested data with no data truncation.
- **Operations:** Spark connects to the AWS Glue metastore to read predefined schemas. Audit metadata (`_ingestion_timestamp`, `_source_file`, `_batch_id`, `_ingestion_date`) is injected.
- **Storage:** Stored in S3 using Delta format, enabling ACID transactions on the data lake.

## 🥈 Silver Layer Transformations and Responsibilities
The Silver layer is responsible for enforcing schema validation and data hygiene.
- **Handling Nulls:** Drops rows with missing `patient_id` and fills categorical nulls with `'Unknown'`.
- **Standardization:** Normalizes values for `gender` (M/F ➔ Male/Female) and `smoking_status`.
- **Range Validation:** Ensures clinical realism (e.g., drops Age > 120, sets outlier BP or BMI measures to Null).
- **Deduplication:** Drops exact duplicates and utilizes Spark `MERGE` to prevent duplicate patient records across daily batch runs.

## 🥇 Gold Layer Analytics
Contains four highly-optimized analytics tables:
1. **Patient Risk Scores:** Calculates a unified risk score (0-100) based on clinical factors (Age, BP, Diabetes, BMI, etc.) and categorizes patients into High, Medium, or Low risk.
2. **Hospital Statistics:** Aggregates total patients, doctors, average vitals, and morbidity percentages (e.g., heart disease rate) partitioned by `hospital_id`.
3. **Diagnosis Analytics:** Summarizes patient counts and average clinical metrics grouped by respective `diagnosis_code`.
4. **Daily KPI Summary:** Maintains a historical record of system-wide processing metrics and clinical averages per reporting date.

## 🔄 Key Data Transformations
- **Feature Engineering:** Synthetic calculated column `risk_score` utilizing conditional weights.
- **Incremental Reads:** PySpark dynamically fetches data matching `MAX(_ingestion_date)` from Bronze.
- **Upserts:** Used Delta Lake's `whenMatchedUpdateAll()` and `whenNotMatchedInsertAll()`.
- **Data Quality Framework:** Custom PySpark functions to measure null rates, duplicate thresholds, and out-of-range clinical values prior to Silver commit.

## 🛠️ Technologies Used
- **Compute & Orchestration:** Databricks, PySpark, Databricks Workflows
- **Storage & Formats:** AWS S3, Delta Lake, Parquet
- **Data Cataloging:** AWS Glue (Crawlers & Data Catalog)
- **IAM & Security:** AWS IAM (Custom Roles and Policies)
- **Languages:** Python (PySpark API), Spark SQL

## 🛡️ Data Quality and Error Handling
- **Automated Validation:** A discrete Data Quality framework validates threshold constraints (e.g., max 5% null rate on Systolic BP) and halts the pipeline if critical thresholds are breached.
- **Event Logging:** Centralized `pipeline_log` Delta table captures pipeline duration, row counts, and explicit error messages with UUID trace IDs.
- **Try/Catch Implementation:** PySpark scripts are encapsulated in Python `try/except` execution blocks to ensure Workflows capture hard failures accurately.

## ⏱️ Pipeline Scheduling
The entire ETL workflow is mapped out as a Directed Acyclic Graph (DAG) using **Databricks Workflows**.
- **Dependencies:** Task DAG established: `bronze_ingestion_task` ➔ `silver_transformation_task` ➔ `gold_analytics_task`.
- **Schedule:** Automated to run daily at `0 2 * * *` (2:00 AM UTC).
- **Alerting:** Configured custom email failure alerting for job states.
