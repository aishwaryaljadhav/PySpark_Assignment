import json
import pytest
from question4.util import *


SAMPLE_JSON = {
    "id": 1001,
    "properties": {"name": "ABC Pvt Ltd", "storeSize": "Medium"},
    "employees": [
        {"empId": 1001, "empName": "Divesh"},
        {"empId": 1002, "empName": "Rajesh"},
        {"empId": 1003, "empName": "David"},
    ],
}


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("Q4_Test").master("local[*]").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture(scope="session")
def json_path(tmp_path_factory):
    p = tmp_path_factory.mktemp("data") / "test.json"
    p.write_text(json.dumps(SAMPLE_JSON))
    return str(p)

@pytest.fixture
def raw_df(spark, json_path):
    return read_json_dynamic(spark, json_path)

@pytest.fixture
def flat_df(raw_df):
    return flatten_df(raw_df)

def test_flatten_df(raw_df, flat_df):
    assert flat_df.count() > raw_df.count()

    expected_cols = {"id", "name", "storeSize", "empId", "empName"}
    assert expected_cols.issubset(set(flat_df.columns))

def test_compare_counts(raw_df, flat_df):
    orig, flat = compare_counts(raw_df, flat_df)
    assert flat > orig

def test_filter_by_id(raw_df):
    result = filter_by_id(raw_df)
    assert result.count() == 1

def test_rename_camel_to_snake(flat_df):
    result = rename_camel_to_snake(flat_df)

    assert "emp_id" in result.columns
    assert "emp_name" in result.columns
    assert "store_size" in result.columns

def test_add_date_columns(flat_df):
    result = add_date_columns(flat_df)

    assert "load_date" in result.columns
    assert "year" in result.columns
    assert "month" in result.columns
    assert "day" in result.columns
    assert result.schema["load_date"].dataType == DateType()

def test_explode_variants(raw_df):
    df1 = demo_explode(raw_df)
    df2 = demo_explode_outer(raw_df)
    df3 = demo_posexplode(raw_df)

    assert "emp" in df1.columns
    assert "emp" in df2.columns
    assert "emp" in df3.columns
    assert "pos" in df3.columns

def test_write_to_emp_table(spark, flat_df):
    df = add_date_columns(flat_df)

    write_to_emp_table(df, spark)

    tables = spark.sql("SHOW TABLES IN employee").collect()
    table_names = [row.tableName for row in tables]

    assert "employee_details" in table_names