import pytest
from question1.util import *

@pytest.fixture(scope="session")
def spark():
    spark = get_spark()
    yield spark
    spark.stop()

@pytest.fixture
def purchase_df(spark):
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
    return spark.createDataFrame(data, ["customer", "product_model"])

@pytest.fixture
def product_df(spark):
    data = [
        ("iphone13",),
        ("dell i5 core",),
        ("dell i3 core",),
        ("hp i5 core",),
        ("iphone14",),
    ]
    return spark.createDataFrame(data, ["product_model"])

def test_customers_only_iphone13(purchase_df):
    result = only_iphone13(purchase_df)
    customer_ids = [row.customer for row in result.collect()]

    assert 4 in customer_ids
    assert 1 not in customer_ids

def test_customers_upgraded(purchase_df):
    result = upgraded_iphone13_to_iphone14(purchase_df)
    customer_ids = [row.customer for row in result.collect()]

    assert 1 in customer_ids
    assert 3 in customer_ids
    assert 4 not in customer_ids

def test_customers_bought_all_products(purchase_df, product_df):
    result = bought_all_products(purchase_df, product_df)
    customer_ids = [row.customer for row in result.collect()]

    assert 1 in customer_ids
    assert 4 not in customer_ids