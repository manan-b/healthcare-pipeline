# import dlt
# from pyspark.sql.functions import col, current_timestamp

# # Base schema for your tables
# CATALOG = "healthcare-org-catalog"
# SCHEMA = "healthcare-org-schema"

# # =====================================================
# # 1️⃣ HOSPITAL ERROR CHECK
# # =====================================================
# @dlt.table(
#     name="hospital_error_logs",
#     comment="Rows from hospital table failing quality rules"
# )
# @dlt.expect("hospital_id_not_null", "hospital_id IS NOT NULL")
# def hospital_error():
#     df = spark.table(f"{CATALOG}.{SCHEMA}.hospital")
#     return df.withColumn("ingestion_timestamp", current_timestamp()) \
#              .withColumn("source_file", col("_metadata.file_path"))

# # =====================================================
# # 2️⃣ DEMOGRAPHICS ERROR CHECK
# # =====================================================
# @dlt.table(
#     name="demographics_error_logs",
#     comment="Rows from demographics table failing quality rules"
# )
# @dlt.expect("patient_id_not_null", "patient_id IS NOT NULL")
# def demographics_error():
#     df = spark.table(f"{CATALOG}.{SCHEMA}.demographics")
#     return df.withColumn("ingestion_timestamp", current_timestamp()) \
#              .withColumn("source_file", col("_metadata.file_path"))

# # =====================================================
# # 3️⃣ DIAGNOSIS ERROR CHECK
# # =====================================================
# @dlt.table(
#     name="diagnosis_error_logs",
#     comment="Rows from diagnosis table failing quality rules"
# )
# @dlt.expect("diagnosis_code_not_null", "diagnosis_code IS NOT NULL")
# def diagnosis_error():
#     df = spark.table(f"{CATALOG}.{SCHEMA}.diagnosis")
#     return df.withColumn("ingestion_timestamp", current_timestamp()) \
#              .withColumn("source_file", col("_metadata.file_path"))

# # =====================================================
# # 4️⃣ LAB ERROR CHECK
# # =====================================================
# @dlt.table(
#     name="lab_error_logs",
#     comment="Rows from lab table failing quality rules"
# )
# @dlt.expect("lab_result_not_null", "lab_result IS NOT NULL")
# def lab_error():
#     df = spark.table(f"{CATALOG}.{SCHEMA}.lab")
#     return df.withColumn("ingestion_timestamp", current_timestamp()) \
#              .withColumn("source_file", col("_metadata.file_path"))

# # =====================================================
# # 5️⃣ VITALS ERROR CHECK
# # =====================================================
# @dlt.table(
#     name="vitals_error_logs",
#     comment="Rows from vitals table failing quality rules"
# )
# @dlt.expect("heart_rate_not_null", "heart_rate IS NOT NULL")
# def vitals_error():
#     df = spark.table(f"{CATALOG}.{SCHEMA}.vitals")
#     return df.withColumn("ingestion_timestamp", current_timestamp()) \
#              .withColumn("source_file", col("_metadata.file_path"))