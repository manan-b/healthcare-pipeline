# ============================================================
# PIPELINE CONFIGURATION
# ============================================================
# Edit the values below to match YOUR setup before running any notebook.
# This file is imported by all pipeline notebooks.
# ============================================================

# -----------------------------------------------
# AWS S3 Configuration
# -----------------------------------------------
S3_BUCKET_NAME = "your-s3-bucket-name"     # e.g. "revature-health-data"
S3_BUCKET      = f"s3a://{S3_BUCKET_NAME}"

RAW_PATH       = f"{S3_BUCKET}/raw/"
BRONZE_PATH    = f"{S3_BUCKET}/bronze/patient_records"
SILVER_PATH    = f"{S3_BUCKET}/silver/patient_records"
GOLD_PATH      = f"{S3_BUCKET}/gold"
LOG_PATH       = f"{S3_BUCKET}/gold/pipeline_log"

RAW_CSV_FILE   = "patient_records.csv"          # filename in raw/ folder
RAW_CSV_PATH   = f"{RAW_PATH}{RAW_CSV_FILE}"

# -----------------------------------------------
# AWS Glue Configuration
# -----------------------------------------------
GLUE_DATABASE  = "healthcare_raw"               # Glue Data Catalog database name
GLUE_TABLE     = "patient_records"              # Table name created by Glue Crawler

# -----------------------------------------------
# Databricks / Delta Lake Configuration
# -----------------------------------------------
DATABASE_NAME  = "healthcare"                   # Hive Metastore database name

# Delta table full names
BRONZE_TABLE   = f"{DATABASE_NAME}.bronze_patient_records"
SILVER_TABLE   = f"{DATABASE_NAME}.silver_patient_records"

# Gold tables
GOLD_RISK_TABLE     = f"{DATABASE_NAME}.gold_patient_risk"
GOLD_HOSP_TABLE     = f"{DATABASE_NAME}.gold_hospital_statistics"
GOLD_DIAG_TABLE     = f"{DATABASE_NAME}.gold_diagnosis_analytics"
GOLD_KPI_TABLE      = f"{DATABASE_NAME}.gold_daily_kpi"
PIPELINE_LOG_TABLE  = f"{DATABASE_NAME}.pipeline_log"

# Gold physical paths
GOLD_RISK_PATH      = f"{GOLD_PATH}/patient_risk_scores"
GOLD_HOSP_PATH      = f"{GOLD_PATH}/hospital_statistics"
GOLD_DIAG_PATH      = f"{GOLD_PATH}/diagnosis_analytics"
GOLD_KPI_PATH       = f"{GOLD_PATH}/daily_kpi_summary"

# -----------------------------------------------
# AWS Access Keys (set here OR in cluster Spark config)
# SECURITY WARNING: Do NOT commit actual keys to GitHub.
# Use environment variables or Databricks Secrets instead.
# -----------------------------------------------
AWS_ACCESS_KEY = "YOUR_ACCESS_KEY_ID"       # Replace before running
AWS_SECRET_KEY = "YOUR_SECRET_ACCESS_KEY"   # Replace before running
AWS_REGION     = "us-east-1"

print("Pipeline configuration loaded.")
print(f"  S3 Bucket    : {S3_BUCKET}")
print(f"  Glue DB      : {GLUE_DATABASE}.{GLUE_TABLE}")
print(f"  Delta DB     : {DATABASE_NAME}")
