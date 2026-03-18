from util import *


def main():
    spark = get_spark()

    print("Create all 3 DataFrames with dynamic schema")
    employee_df = create_employee_df(spark)
    department_df = create_department_df(spark)
    country_df = create_country_df(spark)
    employee_df.show()
    department_df.show()
    country_df.show()

    print("Avg salary per department")
    avg_salary(employee_df).show()

    print("name starts with 'm'")
    starts_with_m(employee_df, department_df).show()

    print("bonus column")
    emp_with_bonus = bonus_column(employee_df)
    emp_with_bonus.show()

    print("Reorder columns")
    reorder_column(employee_df).show()

    print("joins")
    joins = perform_joins(employee_df, department_df)
    for join_type, result_df in joins.items():
        result_df.show()

    print("Replace State with country_name")
    with_country = state_with_country(employee_df, country_df)
    with_country.show()

    print("Lowercase columns and adding load_date")
    final_df = lc_col_and_date(with_country)
    final_df.show()

    print("Write external tables")
    write_external_tables(final_df, spark)

    print(spark.conf.get("spark.sql.warehouse.dir"))

    spark.stop()


if __name__ == "__main__":
    main()
