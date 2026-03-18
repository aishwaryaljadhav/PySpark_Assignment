from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def get_spark():
    spark = SparkSession.builder.appName("UserActivity").enableHiveSupport().getOrCreate()
    return spark

data = [
 (1, 101, 'login', '2023-09-05 08:30:00'),
 (2, 102, 'click', '2023-09-06 12:45:00'),
 (3, 101, 'click', '2023-09-07 14:15:00'),
 (4, 103, 'login', '2023-09-08 09:00:00'),
 (5, 102, 'logout', '2023-09-09 17:30:00'),
 (6, 101, 'click', '2023-09-10 11:20:00'),
 (7, 103, 'click', '2023-09-11 10:15:00'),
 (8, 102, 'click', '2023-09-12 13:10:00')
]

schema = StructType([
    StructField("log id", IntegerType(), True),
    StructField("user$id", IntegerType(), True),
    StructField("action", StringType(), True),
    StructField("timestamp", StringType(), True)
])

COLUMN_RENAME= {
    "log id": "log_id",
    "user$id": "user_id",
    "action": "user_activity",
    "timestamp": "time_stamp"
}

def create_df(spark):
    return spark.createDataFrame(data, schema)

def rename_columns(df, rename_map):

    for old_name, new_name in rename_map.items():
        df = df.withColumnRenamed(old_name, new_name)

    return df

def last_7_days(df):

    df2 = df.withColumn(
        "login_date",
        to_date(col("time_stamp"), "yyyy-MM-dd HH:mm:ss")
    )

    max_date = df2.selectExpr("max(login_date) as max_date").collect()[0][0]

    result = (
        df2
        .filter(datediff(lit(max_date), col("login_date")) <= 7)
        .groupBy("user_id")
        .agg(count("user_activity").alias("action_count"))
    )

    return result

def login_date_column(df):

    return df.withColumn(
        "login_date",
        to_date(col("time_stamp"), "yyyy-MM-dd HH:mm:ss")
    ).drop("time_stamp")

def write_as_csv(df, output_path="q3_output"):

    df.write.mode("overwrite") \
        .option("header", "true") \
        .option("dateFormat", "yyyy-MM-dd") \
        .csv(output_path)

def managed_table(df, spark):

    spark.sql("CREATE DATABASE IF NOT EXISTS user")
    df.write.mode("overwrite").saveAsTable("user.login_details")