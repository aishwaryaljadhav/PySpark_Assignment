from util import *

JSON_FILE_PATH = "nested_json_file.json"


def main():

    spark = get_spark()

    print("Reading JSON")
    df = read_json_dynamic(spark, JSON_FILE_PATH)
    df.printSchema()
    df.show(truncate=False)

    print("Flatten Data")
    flat_df = flatten_df(df)
    flat_df.printSchema()
    flat_df.show(truncate=False)

    print("Compare counts")
    compare_counts(df, flat_df)

    print("Explode")
    demo_explode(df).show(truncate=False)

    print("Explode Outer")
    demo_explode_outer(df).show(truncate=False)

    print("Posexplode")
    demo_posexplode(df).show(truncate=False)

    print("Filter id = 1001")
    filter_by_id(df).show(truncate=False)

    print("Rename columns")
    snake_df = rename_camel_to_snake(flat_df)
    snake_df.show(truncate=False)

    print("Add date columns")
    final_df = add_date_columns(snake_df)
    final_df.show(truncate=False)

    print("Write to table")
    write_to_emp_table(final_df, spark)

    spark.stop()


if __name__ == "__main__":
    main()