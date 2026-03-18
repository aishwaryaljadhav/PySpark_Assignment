from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def get_spark():
    spark = SparkSession.builder.appName("CreditCard").getOrCreate()
    return spark

card_data = [
    ("1234567891234567",),
    ("5678912345671234",),
    ("9123456712345678",),
    ("1234567812341122",),
    ("1234567812341342",)
]

schema = StructType([
    StructField("card_number", StringType(), True)
])


def create_df_from_list(spark):
    return spark.createDataFrame(card_data, schema)

def create_df_from_rdd(spark):
    rdd = spark.sparkContext.parallelize(card_data)
    return spark.createDataFrame(rdd, schema)

def get_partition_count(df):
    return df.rdd.getNumPartitions()

def increase_partitions(df, n):
    return df.repartition(n)

def decrease_partitions(df, n):
    return df.repartition(n)

def mask_card(card):
    if card is None:
        return None
    last4 = card[-4:]
    return "*" * (len(card) - 4) + last4

mask_udf = udf(mask_card, StringType())

def add_masked_column(df):
    return df.withColumn("masked_card_number", mask_udf(col("card_number")))