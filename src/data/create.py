from pyspark import SparkConf, SparkContext
import pyspark
import datetime as dt


def create(
    sc,
    n_customers=5000, 
    n_terminals=10000, 
    nb_days=30, 
    start_date=dt.date(year=2022, month=1, day=1), 
    radius=10,
):
    from src.emulator import generate_dataset, add_frauds, generate_transactions_table

    customer_profiles_table_df, terminal_profiles_table_df, transactions_df = \
        generate_dataset(n_customers, n_terminals, nb_days, start_date, radius)

    transactions_df = add_frauds(customer_profiles_table_df, terminal_profiles_table_df, transactions_df)

    spark = pyspark.sql.SparkSession.builder.getOrCreate()

    transactions_sdf=spark.createDataFrame(transactions_df)
    customer_profiles_table_sdf=spark.createDataFrame(customer_profiles_table_df)
    terminal_profiles_table_sdf=spark.createDataFrame(terminal_profiles_table_df)

    transactions_sdf.write.mode("append").parquet("hdfs:///user/otus/card-fraud-detection/transactions.parquet")
    customer_profiles_table_sdf.write.mode("append").parquet("hdfs:///user/otus/card-fraud-detection/customer_profiles_table.parquet")
    terminal_profiles_table_sdf.write.mode("append").parquet("hdfs:///user/otus/card-fraud-detection/terminal_profiles_table.parquet") 


if __name__ == '__main__':
    import sys

    conf = SparkConf().setAppName("Create dataset")
    sc = SparkContext(conf=conf)
    n_customers, n_terminals, nb_days, start_date, radius = sys.argv[1:]

    create(
        sc, 
        int(n_customers), 
        int(n_terminals), 
        int(nb_days), 
        dt.datetime.strptime(start_date, "%Y-%m-%d"), 
        int(radius),
    )

