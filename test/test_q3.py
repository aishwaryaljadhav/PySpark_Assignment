import pytest
from question3.util import *


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("Q3_Test").master("local[*]").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def raw_df(spark):
    return create_df(spark)

@pytest.fixture
def renamed_df(raw_df):
    return rename_columns(raw_df, COLUMN_RENAME)

def test_rename_columns(renamed_df):
    expected_cols = {"log_id", "user_id", "user_activity", "time_stamp"}

    assert expected_cols.issubset(set(renamed_df.columns))
    assert "log id" not in renamed_df.columns
    assert "user$id" not in renamed_df.columns

def test_login_date_column(renamed_df):
    result = login_date_column(renamed_df)

    assert "login_date" in result.columns
    assert result.schema["login_date"].dataType == DateType()
    assert "time_stamp" not in result.columns

def test_last_7_days(renamed_df):
    result = last_7_days(renamed_df)

    assert "user_id" in result.columns
    assert "action_count" in result.columns