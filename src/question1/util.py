from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def get_spark():
    spark = SparkSession.builder.appName("CustomerPurchase").getOrCreate()
    return spark

def purchase_dataf(spark):
    data = [
        (1, "iphone13"),
        (1, "dell i5 core"),
        (2, "iphone13"),
        (2, "dell i5 core"),
        (3, "iphone13"),
        (3, "dell i5 core"),
        (1, "dell i3 core"),
        (1, "hp i5 core"),
        (1, "iphone14"),
        (3, "iphone14"),
        (4, "iphone13"),
    ]

    purchase_df = spark.createDataFrame(data, ["customer", "product_model"])
    return purchase_df

def product_dataf(spark):

    data = [
        ("iphone13",),
        ("dell i5 core",),
        ("dell i3 core",),
        ("hp i5 core",),
        ("iphone14",),
    ]

    product_df = spark.createDataFrame(data, ["product_model"])
    return product_df

def only_iphone13(purchase_df):

    customer_products = purchase_df.groupBy("customer").agg(
        collect_set("product_model").alias("products")
    )

    result = customer_products.filter(
        (array_contains(col("products"), "iphone13")) &
        (size(col("products")) == 1)
    ).select("customer")

    return result

def upgraded_iphone13_to_iphone14(purchase_df):

    customer_products = purchase_df.groupBy("customer").agg(
        collect_set("product_model").alias("products")
    )

    result = customer_products.filter(
        array_contains(col("products"), "iphone13") &
        array_contains(col("products"), "iphone14")
    ).select("customer")

    return result

def bought_all_products(purchase_df, product_df):
    total_products = product_df.count()

    customer_products = purchase_df.groupBy("customer").agg(
        collect_set("product_model").alias("products")
    )

    result = customer_products.filter(
        size(col("products")) == total_products
    ).select("customer")

    return result