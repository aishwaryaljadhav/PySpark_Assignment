from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def get_spark():
    spark = SparkSession.builder \
        .appName("Q5_Employee") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def employee_schema():
    return StructType([
        StructField("employee_id", IntegerType(), True),
        StructField("employee_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("State", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("Age", IntegerType(), True),
    ])

def department_schema():
    return StructType([
        StructField("dept_id", StringType(), True),
        StructField("dept_name", StringType(), True),
    ])

def country_schema():
    return StructType([
        StructField("country_code", StringType(), True),
        StructField("country_name", StringType(), True),
    ])

def create_employee_df(spark):
    data = [
        (11, "james", " D101", "ny", 9000, 34),
        (12, "michel", " D101", "ny", 8900, 32),
        (13, "robert", " D102", "ca", 7900, 29),
        (14, "scott", " D103", "ca", 8000, 36),
        (15, "jen", " D102", "ny", 9500, 38),
        (16, "jeff", " D103", "uk", 9100, 35),
        (17, "maria", " D101", "ny", 7900, 40),
    ]
    return spark.createDataFrame(data, employee_schema())

def create_department_df(spark):
    data = [
        ("D101", "sales"),
        ("D102", "finance"),
        ("D103", "marketing"),
        ("D104", "hr"),
        ("D105", "support"),
    ]
    return spark.createDataFrame(data, department_schema())

def create_country_df(spark):
    data = [
        ("ny", "newyork"),
        ("ca", "california"),
        ("uk", "russia"),
    ]
    return spark.createDataFrame(data, country_schema())

def avg_salary(emp_df):
    return emp_df.groupBy("department").agg(avg("salary").alias("avg_salary"))

def starts_with_m(emp_df, dept_df):

    df = emp_df.join(
        dept_df,
        trim(emp_df["department"]) == dept_df["dept_id"],
        "inner"
    )

    return df.filter(
        col("employee_name").startswith("m") |
        col("dept_name").startswith("m")
    ).select("employee_name", "dept_name")

def bonus_column(emp_df):
    return emp_df.withColumn("bonus", col("salary") * 2)

def reorder_column(emp_df):
    return emp_df.select(
        "employee_id", "employee_name", "salary", "State", "Age", "department"
    )

def perform_joins(emp_df, dept_df):

    result = {}

    condition = trim(emp_df["department"]) == dept_df["dept_id"]

    result["inner"] = emp_df.join(dept_df, condition, "inner")
    result["left"] = emp_df.join(dept_df, condition, "left")
    result["right"] = emp_df.join(dept_df, condition, "right")

    return result

def state_with_country(emp_df, country_df):

    df = emp_df.join(
        country_df,
        emp_df["State"] == country_df["country_code"],
        "left"
    )

    df = df.drop("State", "country_code")
    df = df.withColumnRenamed("country_name", "State")

    return df

def lc_col_and_date(df):

    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())

    df = df.withColumn("load_date", current_date())

    return df

def write_external_tables(df, spark):

    spark.sql("CREATE DATABASE IF NOT EXISTS employee_db")

    df.write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .option("path", "employee_db_csv") \
        .saveAsTable("employee_db.employee_csv")

    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("path", "D:/Aishwarya/employee_db_parquet") \
        .saveAsTable("employee_db.employee_parquet")