from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def get_spark():
    spark = SparkSession.builder \
        .appName("JSON") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def read_json_dynamic(spark, path):
    df = spark.read.option("multiline", "true").json(path)
    return df

def flatten_df(df):

    flat_df = df.withColumn("emp", explode(col("employees"))) \
        .select(
            col("id"),
            col("properties.name").alias("name"),
            col("properties.storeSize").alias("storeSize"),
            col("emp.empId").alias("empId"),
            col("emp.empName").alias("empName")
        )

    return flat_df

def compare_counts(df, flat_df):

    original_count = df.count()
    flat_count = flat_df.count()
    print("Difference :", flat_count - original_count)

    return original_count, flat_count

def demo_explode(df):
    return df.select(col("id"), explode(col("employees")).alias("emp"))

def demo_explode_outer(df):
    return df.select(col("id"), explode_outer(col("employees")).alias("emp"))

def demo_posexplode(df):
    return df.select(col("id"), posexplode(col("employees")).alias("pos", "emp"))

def filter_by_id(df):
    return df.filter(col("id") == 1001)

def rename_camel_to_snake(df):

    for c in df.columns:
        new_name = ""
        for ch in c:
            if ch.isupper():
                new_name = new_name + "_" + ch.lower()
            else:
                new_name = new_name + ch

        if new_name.startswith("_"):
            new_name = new_name[1:]

        df = df.withColumnRenamed(c, new_name)

    return df

def add_date_columns(df):

    df = df.withColumn("load_date", current_date())
    df = df.withColumn("year", year(col("load_date")))
    df = df.withColumn("month", month(col("load_date")))
    df = df.withColumn("day", dayofmonth(col("load_date")))

    return df

def write_to_emp_table(df, spark):

    spark.sql("CREATE DATABASE IF NOT EXISTS employee")

    df.write.mode("overwrite").format("json") \
        .partitionBy("year", "month", "day") \
        .saveAsTable("employee.employee_details")
