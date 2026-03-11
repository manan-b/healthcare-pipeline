import dlt
from pyspark.sql.functions import current_timestamp, col

# =====================================================
# 1️⃣ HOSPITAL
# =====================================================

@dlt.table(
    name="hospital",
    comment="Bronze Hospital Table"
)
@dlt.expect_or_drop("valid_hospital_id", "hospital_id IS NOT NULL")
def hospital():

    df = spark.read.format("parquet").load(
        "s3://healthcare-patient-analytics-pipeline-src-bkt-s3/processed/hospital/"
    )

    return df.withColumn("ingestion_timestamp", current_timestamp()) \
             .withColumn("source_file", col("_metadata.file_path"))


# =====================================================
# 2️⃣ DEMOGRAPHICS
# =====================================================

@dlt.table(
    name="demographics",
    comment="Bronze Demographics Table"
)
@dlt.expect_or_drop("valid_patient_id", "patient_id IS NOT NULL")
def demographics():

    df = spark.read.format("parquet").load(
        "s3://healthcare-patient-analytics-pipeline-src-bkt-s3/processed/demographics/"
    )

    return df.withColumn("ingestion_timestamp", current_timestamp()) \
             .withColumn("source_file", col("_metadata.file_path"))


# =====================================================
# 3️⃣ DIAGNOSIS
# =====================================================

@dlt.table(
    name="diagnosis",
    comment="Bronze Diagnosis Table"
)
@dlt.expect_or_drop("valid_diagnosis_patient", "patient_id IS NOT NULL")
def diagnosis():

    df = spark.read.format("parquet").load(
        "s3://healthcare-patient-analytics-pipeline-src-bkt-s3/processed/diagnosis/"
    )

    return df.withColumn("ingestion_timestamp", current_timestamp()) \
             .withColumn("source_file", col("_metadata.file_path"))


# =====================================================
# 4️⃣ LAB
# =====================================================

@dlt.table(
    name="lab",
    comment="Bronze Lab Table"
)
@dlt.expect_or_drop("valid_lab_patient", "patient_id IS NOT NULL")
def lab():

    df = spark.read.format("parquet").load(
        "s3://healthcare-patient-analytics-pipeline-src-bkt-s3/processed/lab/"
    )

    return df.withColumn("ingestion_timestamp", current_timestamp()) \
             .withColumn("source_file", col("_metadata.file_path"))


# =====================================================
# 5️⃣ VITALS
# =====================================================

@dlt.table(
    name="vitals",
    comment="Bronze Vitals Table"
)
@dlt.expect_or_drop("valid_vitals_patient", "patient_id IS NOT NULL")
def vitals():

    df = spark.read.format("parquet").load(
        "s3://healthcare-patient-analytics-pipeline-src-bkt-s3/processed/vitals/"
    )

    return df.withColumn("ingestion_timestamp", current_timestamp()) \
             .withColumn("source_file", col("_metadata.file_path"))