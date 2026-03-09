# Healthcare Patient Analytics Pipeline

A production-grade batch data pipeline built for healthcare analytics using **AWS Glue**, **Amazon S3**, **Databricks**, **PySpark**, and **Delta Lake**, following the **Medallion Architecture** (Bronze → Silver → Gold).

---

## Architecture Overview

```
patient_records.csv
        |
        v
  [AWS S3 - raw/]
        |
        v
  [AWS Glue Crawler]
  [Glue Data Catalog]
        |
        v
  [Databricks - Bronze Delta Table]   ← Glue-cataloged raw data ingestion
        |
        v
  [Databricks - Silver Delta Table]   ← Cleaned, validated, deduplicated
        |
        v
  [Databricks - Gold Delta Tables]    ← Aggregated KPIs and analytics
        |
        v
  [Databricks Workflows - Daily Run]  ← Scheduled orchestration (2 AM UTC)
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Storage | Amazon S3 |
| Cataloging | AWS Glue Crawler + Data Catalog |
| Processing | Databricks (Apache Spark 3.5.0) |
| Table Format | Delta Lake |
| Language | PySpark (Python 3.x) |
| Orchestration | Databricks Workflows (Cron) |
| Version Control | GitHub |

---

## Project Structure

```
healthcare-pipeline/
├── notebooks/
│   ├── 00_setup_and_config.py
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   └── 03_gold_analytics.py
├── scripts/
│   └── upload_to_s3.py
├── config/
│   └── pipeline_config.py
├── data/
│   └── sample/
│       └── patient_records_sample.csv
├── docs/
│   └── Healthcare_Patient_Analytics_Pipeline_Guide.txt
├── .gitignore
├── LICENSE
└── README.md
```

---

## Data Pipeline Layers

### Bronze Layer (Raw Ingestion)
- AWS Glue Crawler auto-discovers and catalogs raw CSV files from S3
- Databricks reads the Glue-cataloged data and writes to Delta with audit metadata columns (`_ingestion_timestamp`, `_source_file`, `_batch_id`)
- Partitioned by: `_ingestion_date`

### Silver Layer (Cleaned & Conformed)
- Standardizes categorical columns (gender, smoking_status)
- Applies null handling and clinical range validation on health metrics
- MERGE (upsert) ensures no duplicate `patient_id` records
- Partitioned by: `diagnosis_code`

### Gold Layer (Business Analytics)
Four aggregated tables produced:
1. **`gold_patient_risk`** — Patient risk scores (Low/Medium/High)
2. **`gold_hospital_statistics`** — Per-hospital KPIs (disease rates, avg vitals)
3. **`gold_diagnosis_analytics`** — Biomarkers per diagnosis type
4. **`gold_daily_kpi`** — Daily executive summary metrics

---

## Dataset

File: `patient_records.csv` (~56,500 records, 32 columns)

Key columns: `patient_id`, `age`, `gender`, `systolic_bp`, `diastolic_bp`, `cholesterol_total`, `fasting_glucose`, `hba1c`, `heart_rate`, `smoking_status`, `diabetes`, `hypertension`, `hospital_id`, `doctor_id`, `diagnosis_code`, `heart_disease`

---

## Prerequisites

- AWS Account with S3 and Glue access
- Databricks Workspace (AWS-hosted)
- Python 3.x + Git installed locally
- Databricks Runtime 14.3 LTS (Spark 3.5.0)

---

## Setup Instructions

See the detailed step-by-step guide in:
`docs/Healthcare_Patient_Analytics_Pipeline_Guide.txt`

---

## Team

Revature Data Engineering Batch — Project 2 (2026)

---

## License

MIT License — see [LICENSE](LICENSE)
