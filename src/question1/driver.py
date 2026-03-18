from util import *

def main():
    spark = get_spark()
    purchase_df = purchase_dataf(spark)
    product_df = product_dataf(spark)

    purchase_df.show()
    product_df.show()

    print("Customers who bought ONLY iphone13")
    only_iphone13_df = only_iphone13(purchase_df)
    only_iphone13_df.show()

    print("Customers who upgraded from iphone13 to iphone14")
    upgraded_df = upgraded_iphone13_to_iphone14(purchase_df)
    upgraded_df.show()

    print("Customers who bought ALL products")
    all_products_df = bought_all_products(purchase_df, product_df)
    all_products_df.show()

    spark.stop()


if __name__ == "__main__":
    main()
