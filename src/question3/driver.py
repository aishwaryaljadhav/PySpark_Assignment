from util import *

def main():

    spark = get_spark()
    print("Create DataFrame with StructType schema")
    raw_df = create_df(spark)
    raw_df.printSchema()
    raw_df.show()

    print("Rename columns dynamically")
    renamed_df = rename_columns(raw_df, COLUMN_RENAME)
    renamed_df.printSchema()
    renamed_df.show()

    print("Actions per user in last 7 days")
    last7_df = last_7_days(renamed_df)
    last7_df.show()

    print("Convert timestamp to login_date")
    dated_df = login_date_column(renamed_df)
    dated_df.printSchema()
    dated_df.show()

    print("Write DataFrame as CSV")
    write_as_csv(dated_df)

    print("Write as managed table")
    managed_table(dated_df, spark)

    spark.stop()


if __name__ == "__main__":
    main()