import pytest
from question2.util import *

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[*]").appName("Q2_Test").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def df(spark):
    return create_df_from_list(spark)

def test_partition_increase(df):
    df_new = increase_partitions(df, 5)
    assert get_partition_count(df_new) == 5

def test_partition_decrease(df):
    original = get_partition_count(df)

    df_new = increase_partitions(df, 5)
    df_back = decrease_partitions(df_new, original)

    assert get_partition_count(df_back) == original

def test_mask_card_logic():
    assert mask_card("1234567891234567") == "************4567"
    assert mask_card("5678912345671234") == "************1234"
    assert mask_card(None) is None

def test_add_masked_column(df):
    result = add_masked_column(df)

    assert "masked_card_number" in result.columns

    rows = result.collect()
    for row in rows:
        assert row["masked_card_number"][-4:] == row["card_number"][-4:]
        assert len(row["masked_card_number"]) == len(row["card_number"])