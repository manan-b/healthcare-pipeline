import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid

# -------------------------
# Helper Functions
# -------------------------

def clean_string(column):
    return initcap(trim(regexp_replace(col(column), "[^a-zA-Z0-9 ]", "")))

def clean_id(column):
    return col(column).cast(LongType())

def generate_hex():
    return uuid.uuid4().hex[:12]

hex_udf = udf(lambda: generate_hex(), StringType())

def fix_hospital_name(column):
    return when(
        (col(column).isNull()) |
        (lower(col(column)) == "nan") |
        (trim(col(column)) == ""),
        "Unknown"
    ).otherwise(clean_string(column))

def fix_doctor_name(column):
    return when(
        (col(column).isNull()) |
        (lower(col(column)) == "nan") |
        (trim(col(column)) == ""),
        "Unknown"
    ).otherwise(clean_string(column))

# -------------------------
# City Mapping
# -------------------------

def map_city(column):
    return when(col(column)=="A","Ahmedabad") \
    .when(col(column)=="B","Bengaluru") \
    .when(col(column)=="C","Chennai") \
    .when(col(column)=="D","Dehradun") \
    .when(col(column)=="E","Hyderabad") \
    .otherwise(col(column))

def state_from_city(column):
    return when(col(column)=="Ahmedabad","Gujarat") \
    .when(col(column)=="Bengaluru","Karnataka") \
    .when(col(column)=="Chennai","Tamil Nadu") \
    .when(col(column)=="Dehradun","Uttarakhand") \
    .when(col(column)=="Hyderabad","Telangana")

# =========================================================
# Bronze Views (Prevents duplication)
# =========================================================

@dlt.view
def bronze_demographics():
    return spark.read.table(
        "`healthcare-org-catalog`.`healthcare-org-schema`.demographics"
    )

@dlt.view
def bronze_hospital():
    return spark.read.table(
        "`healthcare-org-catalog`.`healthcare-org-schema`.hospital"
    )

@dlt.view
def bronze_diagnosis():
    return spark.read.table(
        "`healthcare-org-catalog`.`healthcare-org-schema`.diagnosis"
    )

@dlt.view
def bronze_vitals():
    return spark.read.table(
        "`healthcare-org-catalog`.`healthcare-org-schema`.vitals"
    )

@dlt.view
def bronze_lab():
    return spark.read.table(
        "`healthcare-org-catalog`.`healthcare-org-schema`.lab"
    )

# =========================================================
# Silver Tables
# =========================================================

@dlt.table(name="silver_demographics")
def silver_demographics():

    df = dlt.read("bronze_demographics")

    df = df \
    .withColumn("patient_id", clean_id("patient_id")) \
    .filter(col("patient_id").isNotNull()) \
    .withColumn("patient_name", clean_string("patient_name")) \
    .withColumn("age", round(col("age")).cast(IntegerType())) \
    .withColumn("age", when(col("age") > 90, 90).otherwise(col("age"))) \
    .withColumn(
        "gender",
        when(lower(col("gender")).isin("m","male"), "Male")
        .when(lower(col("gender")).isin("f","female"), "Female")
        .otherwise("Other")
    ) \
    .withColumn("city", map_city("city")) \
    .withColumn("state", state_from_city("city")) \
    .withColumn("record_date", to_timestamp("record_date"))

    return df.dropDuplicates()

# ---------------------------------------------------------

@dlt.table(name="silver_hospital")
def silver_hospital():

    df = dlt.read("bronze_hospital")

    df = df \
    .withColumn("hospital_id", hex_udf()) \
    .withColumn("hospital_name", fix_hospital_name("hospital_name")) \
    .withColumn("bed_capacity", round(col("bed_capacity")).cast("int")) \
    .withColumn("icu_beds", round(col("icu_beds")).cast("int")) \
    .withColumn("staff_count", round(col("staff_count")).cast("int")) \
    .withColumn("city", map_city("city")) \
    .withColumn("state", state_from_city("city"))

    df = df.fillna({
        "bed_capacity":0,
        "icu_beds":0,
        "staff_count":0
    })

    return df.dropDuplicates()

# ---------------------------------------------------------

@dlt.table(name="silver_diagnosis")
def silver_diagnosis():

    df = dlt.read("bronze_diagnosis")

    df = df \
    .withColumn("patient_id", clean_id("patient_id")) \
    .filter(col("patient_id").isNotNull()) \
    .withColumn("hospital_name", fix_hospital_name("hospital_name")) \
    .withColumn("doctor_name", fix_doctor_name("doctor_name")) \
    .withColumn("diagnosis_code", upper(trim(col("diagnosis_code")))) \
    .withColumn("record_date", to_timestamp("record_date"))

    return df.dropDuplicates()

# ---------------------------------------------------------

@dlt.table(name="silver_vitals")
def silver_vitals():

    df = dlt.read("bronze_vitals")

    df = df \
    .withColumn("patient_id", clean_id("patient_id")) \
    .filter(col("patient_id").isNotNull()) \
    .withColumn("hospital_name", fix_hospital_name("hospital_name")) \
    .withColumn("heart_rate", abs(col("heart_rate"))) \
    .withColumn("body_temp", abs(col("body_temp"))) \
    .withColumn("bmi", round((rand()*12)+18,2))

    return df.dropDuplicates()

# ---------------------------------------------------------

@dlt.table(name="silver_lab")
def silver_lab():

    df = dlt.read("bronze_lab")

    df = df \
    .withColumn("patient_id", clean_id("patient_id")) \
    .filter(col("patient_id").isNotNull()) \
    .withColumn("hospital_name", fix_hospital_name("hospital_name")) \
    .withColumn("lab_test_name", clean_string("lab_test_name")) \
    .withColumn("technician_name", clean_string("technician_name")) \
    .withColumn("sodium", col("sodium").cast(DoubleType())) \
    .withColumn("calcium", col("calcium").cast(DoubleType())) \
    .withColumn("platelets", col("platelets").cast(DoubleType()))

    return df.dropDuplicates()