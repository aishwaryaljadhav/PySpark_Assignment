import pytest
from question5.util import *


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("Q5_Test").master("local[*]").getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def employee_df(spark):
    return create_employee_df(spark)

@pytest.fixture
def department_df(spark):
    return create_department_df(spark)

@pytest.fixture
def country_df(spark):
    return create_country_df(spark)

def test_avg_salary(employee_df):
    result = avg_salary(employee_df)

    assert "avg_salary" in result.columns
    assert result.count() > 0

def test_bonus_column(employee_df):
    result = bonus_column(employee_df)

    assert "bonus" in result.columns

    row = result.filter("employee_id = 11").collect()[0]
    assert row["bonus"] == row["salary"] * 2

def test_reorder_column(employee_df):
    result = reorder_column(employee_df)

    expected = ["employee_id", "employee_name", "salary", "State", "Age", "department"]
    assert result.columns == expected

def test_perform_joins(employee_df, department_df):
    joins = perform_joins(employee_df, department_df)

    assert "inner" in joins
    assert "left" in joins
    assert "right" in joins

    assert joins["inner"].count() == employee_df.count()
    assert joins["left"].count() >= employee_df.count()

def test_state_with_country(employee_df, country_df):
    result = state_with_country(employee_df, country_df)

    values = [row["State"] for row in result.collect()]

    assert "newyork" in values or "california" in values

def test_lc_col_and_date(employee_df, country_df):
    df = state_with_country(employee_df, country_df)
    result = lc_col_and_date(df)

    for c in result.columns:
        assert c == c.lower()

    assert "load_date" in result.columns
    assert result.schema["load_date"].dataType == DateType()

def test_starts_with_m(employee_df, department_df):
    result = starts_with_m(employee_df, department_df)

    names = [row["employee_name"] for row in result.collect()]
    assert any(n.startswith("m") for n in names)