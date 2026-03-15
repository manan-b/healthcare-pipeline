import dlt
from pyspark.sql.functions import col, avg, count, round, countDistinct

# =====================================================
# METRIC: Health by City
# =====================================================
@dlt.table(
    name="metric_health_by_city",
    comment="Aggregated health metrics and patient counts by city"
)
def metric_health_by_city():
    try:
        fact = dlt.read("fact_patient_health_metrics")
        dim_patient = dlt.read("dim_patient")

        df = fact.join(dim_patient, "patient_id", "inner")

        return df.groupBy("city").agg(
            countDistinct("patient_id").alias("total_patients"),
            round(avg("risk_score"), 2).alias("avg_risk_score"),
            round(avg("treatment_cost"), 2).alias("avg_treatment_cost"),
            round(avg("recovery_days"), 2).alias("avg_recovery_days"),
            round(avg("severity_score"), 2).alias("avg_severity_score")
        )
    except Exception as e:
        print(f"Error in metric_health_by_city: {e}")
        # Return empty dataframe with expected schema
        schema = """
        city STRING, total_patients LONG, avg_risk_score DOUBLE, 
        avg_treatment_cost DOUBLE, avg_recovery_days DOUBLE, avg_severity_score DOUBLE
        """
        return spark.createDataFrame([], schema=schema)


# =====================================================
# METRIC: BMI Profile Analysis
# =====================================================
@dlt.table(
    name="metric_bmi_profile_analysis",
    comment="Analysis of health metrics across different BMI categories"
)
def metric_bmi_profile_analysis():
    try:
        fact = dlt.read("fact_patient_health_metrics")

        return fact.groupBy("bmi_category").agg(
            countDistinct("patient_id").alias("total_patients"),
            round(avg("cholesterol"), 2).alias("avg_cholesterol"),
            round(avg("glucose_level"), 2).alias("avg_glucose_level"),
            round(avg("blood_pressure_sys"), 2).alias("avg_bp_sys"),
            round(avg("blood_pressure_dia"), 2).alias("avg_bp_dia"),
            round(avg("risk_score"), 2).alias("avg_risk_score"),
            round(avg("bmi"), 2).alias("avg_bmi")
        )
    except Exception as e:
        print(f"Error in metric_bmi_profile_analysis: {e}")
        schema = """
        bmi_category STRING, total_patients LONG, avg_cholesterol DOUBLE, 
        avg_glucose_level DOUBLE, avg_bp_sys DOUBLE, avg_bp_dia DOUBLE, 
        avg_risk_score DOUBLE, avg_bmi DOUBLE
        """
        return spark.createDataFrame([], schema=schema)


# =====================================================
# METRIC: Daily Health Trends
# =====================================================
@dlt.table(
    name="metric_daily_health_trends",
    comment="Daily trends for key health indicators"
)
def metric_daily_health_trends():
    try:
        fact = dlt.read("fact_patient_health_metrics")
        dim_date = dlt.read("dim_date")
        
        # Ensure we have the date components
        df = fact.join(dim_date, "record_date", "inner")

        return df.groupBy("record_date", "year", "month").agg(
            count("patient_id").alias("total_records"),
            countDistinct("patient_id").alias("unique_patients"),
            round(avg("oxygen_level"), 2).alias("avg_oxygen_level"),
            round(avg("heart_rate"), 2).alias("avg_heart_rate"),
            round(avg("severity_score"), 2).alias("avg_severity_score")
        ).orderBy(col("record_date").asc())
    except Exception as e:
        print(f"Error in metric_daily_health_trends: {e}")
        schema = """
        record_date DATE, year INT, month INT, total_records LONG, 
        unique_patients LONG, avg_oxygen_level DOUBLE, avg_heart_rate DOUBLE, 
        avg_severity_score DOUBLE
        """
        return spark.createDataFrame([], schema=schema)