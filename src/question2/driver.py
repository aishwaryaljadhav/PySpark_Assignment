from util import *

def main():

    spark = get_spark()

    print("Create DataFrame from list")
    credit_card_df = create_df_from_list(spark)
    credit_card_df.show()

    print("Create DataFrame from RDD")
    df_rdd = create_df_from_rdd(spark)
    df_rdd.show()

    print("Number of partitions")
    original_partitions = get_partition_count(credit_card_df)
    print(original_partitions)

    print("Increase partitions to 5")
    df_increased = increase_partitions(credit_card_df, 5)
    print(get_partition_count(df_increased))

    print("Decrease partitions back to original")
    df_original = decrease_partitions(df_increased, original_partitions)
    print(get_partition_count(df_original))

    print("Masked card numbers")
    result_df = add_masked_column(credit_card_df)
    result_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()