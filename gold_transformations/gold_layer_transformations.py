import dlt
from pyspark.sql.functions import *

# =====================================================
# DIM PATIENT
# =====================================================
@dlt.table(
    name="dim_patient",
    comment="Patient dimension"
)
@dlt.expect_or_drop("valid_patient_id", "patient_id IS NOT NULL")
def dim_patient():

    demo = spark.read.table("LIVE.silver_demographics")

    return demo.select(
        "patient_id",
        "patient_name",
        "gender",
        "city",
        "age"
    ).dropDuplicates(["patient_id"])


# =====================================================
# DIM HOSPITAL
# =====================================================
@dlt.table(
    name="dim_hospital",
    comment="Hospital dimension"
)
@dlt.expect_or_drop("valid_hospital_id", "hospital_id IS NOT NULL")
def dim_hospital():

    hosp = spark.read.table("LIVE.silver_hospital")

    return hosp.select(
        "hospital_id",
        "hospital_name",
        "city",
        "state",
        "bed_capacity",
        "icu_beds"
    ).dropDuplicates(["hospital_id"])


# =====================================================
# DIM DOCTOR
# =====================================================
@dlt.table(
    name="dim_doctor",
    comment="Doctor dimension"
)
@dlt.expect_or_drop("valid_doctor_name", "doctor_name IS NOT NULL")
def dim_doctor():

    diag = spark.read.table("LIVE.silver_diagnosis")

    return diag.select(
        "doctor_name",
        "hospital_name"
    ).dropDuplicates()


# =====================================================
# DIM DATE
# =====================================================
@dlt.table(
    name="dim_date",
    comment="Date dimension"
)
@dlt.expect_or_drop("valid_record_date", "record_date IS NOT NULL")
def dim_date():

    demo = spark.read.table("LIVE.silver_demographics")

    return demo.select(
        col("record_date"),
        year("record_date").alias("year"),
        month("record_date").alias("month"),
        dayofmonth("record_date").alias("day"),
        quarter("record_date").alias("quarter")
    ).dropDuplicates()


# =====================================================
# FACT TABLE
# =====================================================
@dlt.table(
    name="fact_patient_health_metrics",
    comment="Patient health metrics fact table"
)
@dlt.expect_or_drop("valid_patient_id", "patient_id IS NOT NULL")
def fact_patient_health_metrics():

    demo = spark.read.table("LIVE.silver_demographics")
    vitals = spark.read.table("LIVE.silver_vitals")
    lab = spark.read.table("LIVE.silver_lab")
    diag = spark.read.table("LIVE.silver_diagnosis")

    # Aggregate Vitals
    vitals_agg = vitals.groupBy("patient_id").agg(
        avg("heart_rate").alias("heart_rate"),
        avg("blood_pressure_sys").alias("blood_pressure_sys"),
        avg("blood_pressure_dia").alias("blood_pressure_dia"),
        avg("oxygen_level").alias("oxygen_level"),
        avg("cholesterol").alias("cholesterol"),
        avg("glucose_level").alias("glucose_level"),
        avg("bmi").alias("bmi")
    )

    # Aggregate Lab
    lab_agg = lab.groupBy("patient_id").agg(
        avg("sodium").alias("sodium"),
        avg("calcium").alias("calcium"),
        avg("platelets").alias("platelets")
    )

    # Aggregate Diagnosis
    diag_agg = diag.groupBy("patient_id").agg(
        avg("severity_score").alias("severity_score"),
        avg("risk_probability").alias("risk_probability"),
        avg("treatment_cost").alias("treatment_cost"),
        avg("recovery_days").alias("recovery_days")
    )

    # Join Tables
    df = demo \
        .join(vitals_agg, "patient_id", "left") \
        .join(lab_agg, "patient_id", "left") \
        .join(diag_agg, "patient_id", "left")

    # Risk Score
    df = df.withColumn(
        "risk_score",
        (0.3 * coalesce(col("cholesterol"), lit(0))) +
        (0.25 * coalesce(col("glucose_level"), lit(0))) +
        (0.2 * coalesce(col("bmi"), lit(0))) +
        (0.15 * coalesce(col("severity_score"), lit(0))) +
        (0.1 * coalesce(col("age"), lit(0)))
    )

    # BMI Category
    df = df.withColumn(
        "bmi_category",
        when(col("bmi") < 18.5, "Underweight")
        .when(col("bmi") < 25, "Normal")
        .when(col("bmi") < 30, "Overweight")
        .otherwise("Obese")
    )

    return df.select(
        "patient_id",
        "record_date",
        "heart_rate",
        "blood_pressure_sys",
        "blood_pressure_dia",
        "oxygen_level",
        "cholesterol",
        "glucose_level",
        "bmi",
        "bmi_category",
        "severity_score",
        "risk_probability",
        "treatment_cost",
        "recovery_days",
        "risk_score",
        "sodium",
        "calcium",
        "platelets"
    )


# =====================================================
# GOLD METRICS TABLES
# =====================================================

# BMI Distribution
@dlt.table(
    name="gold_bmi_distribution",
    comment="Patient distribution by BMI category"
)
def gold_bmi_distribution():

    fact = spark.read.table("LIVE.fact_patient_health_metrics")

    return fact.groupBy("bmi_category").agg(
        count("*").alias("patient_count"),
        avg("risk_score").alias("avg_risk_score")
    )


# Hospital Performance
@dlt.table(
    name="gold_hospital_performance",
    comment="Hospital performance metrics"
)
def gold_hospital_performance():

    fact = spark.read.table("LIVE.fact_patient_health_metrics")
    diag = spark.read.table("LIVE.silver_diagnosis")

    df = fact.join(diag.select("patient_id","hospital_name"), "patient_id", "left")

    return df.groupBy("hospital_name").agg(
        countDistinct("patient_id").alias("total_patients"),
        avg("risk_score").alias("avg_risk_score"),
        avg("recovery_days").alias("avg_recovery_days"),
        avg("treatment_cost").alias("avg_treatment_cost")
    )


# Overall Health Summary
@dlt.table(
    name="gold_patient_health_summary",
    comment="Overall patient health metrics"
)
def gold_patient_health_summary():

    fact = spark.read.table("LIVE.fact_patient_health_metrics")

    return fact.agg(
        countDistinct("patient_id").alias("total_patients"),
        avg("heart_rate").alias("avg_heart_rate"),
        avg("cholesterol").alias("avg_cholesterol"),
        avg("glucose_level").alias("avg_glucose"),
        avg("bmi").alias("avg_bmi"),
        avg("risk_score").alias("avg_risk_score")
    )


# Risk Distribution
@dlt.table(
    name="gold_risk_distribution",
    comment="Risk category distribution"
)
def gold_risk_distribution():

    fact = spark.read.table("LIVE.fact_patient_health_metrics")

    df = fact.withColumn(
        "risk_category",
        when(col("risk_score") < 30, "Low Risk")
        .when(col("risk_score") < 60, "Medium Risk")
        .otherwise("High Risk")
    )

    return df.groupBy("risk_category").agg(
        count("*").alias("patient_count"),
        avg("treatment_cost").alias("avg_treatment_cost")
    )