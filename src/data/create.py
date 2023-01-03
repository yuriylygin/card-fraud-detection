import click
import datetime as dt


def create(
    sc,
    n_customers=5000, 
    n_terminals=10000, 
    nb_days=30, 
    start_date='2022-01-01', 
    radius=10,
):
    from src.emulator import generate_dataset, add_frauds, generate_transactions_table

    customer_profiles_table_df, terminal_profiles_table_df, transactions_df = \
        generate_dataset(
            n_customers=n_customers, 
            n_terminals=n_terminals, 
            nb_days=nb_days, 
            start_date=dt.strptime(start_date, "%Y-%m-%d"), 
            r=radius,
        )

    transactions_df = add_frauds(customer_profiles_table_df, terminal_profiles_table_df, transactions_df)

    transactions_sdf=sc.createDataFrame(transactions_df)
    customer_profiles_table_sdf=sc.createDataFrame(customer_profiles_table_df)
    terminal_profiles_table_sdf=sc.createDataFrame(terminal_profiles_table_df)

    transactions_sdf.write.mode("append").parquet("hdfs:///user/otus/card-fraud-detection/transactions.parquet")
    customer_profiles_table_sdf.write.mode("append").parquet("hdfs:///user/otus/card-fraud-detection/customer_profiles_table.parquet")
    terminal_profiles_table_sdf.write.mode("append").parquet("hdfs:///user/otus/card-fraud-detection/terminal_profiles_table.parquet") 


if __name__ == '__main__':
    import sys
    from pyspark import SparkConf, SparkContext

    conf = SparkConf().setAppName("Create dataset")
    sc = SparkContext(conf=conf)
    n_customers, n_terminals, nb_days, start_date, radius = sys.argv[1:]
    create(sc, n_customers, n_terminals, nb_days, start_date, radius)

