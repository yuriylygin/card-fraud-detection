from pyspark import SparkConf, SparkContext
import pyspark
import datetime as dt


def update(
    sc, 
    nb_days=30, 
    start_date=dt.date(year=2022, month=2, day=1), 
    radius=5
):
    from src.emulator import add_frauds, generate_transactions_table

    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    customer_profiles_table_sdf = (
        spark.read
        .format("parquet")
        .option("header", True)
        .option("separator", ",")
        .load("hdfs:///user/otus/card-fraud-detection/customer_profiles_table.parquet")
    )
    terminal_profiles_table_sdf = (
        spark.read
        .format("parquet")
        .option("header", True)
        .option("separator", ",")
        .load("hdfs:///user/otus/card-fraud-detection/terminal_profiles_table.parquet")
    )
    customer_profiles_table_df = customer_profiles_table_sdf.toPandas()
    terminal_profiles_table_df = terminal_profiles_table_sdf.toPandas()
    
    transactions_df = (
        customer_profiles_table_df
            .groupby('CUSTOMER_ID')
            .apply(lambda x : generate_transactions_table(x.iloc[0], start_date=start_date,  nb_days=nb_days))
            .reset_index(drop=True)
    )
    transactions_df = add_frauds(customer_profiles_table_df, terminal_profiles_table_df, transactions_df)
    transactions_sdf = spark.createDataFrame(transactions_df) 
    transactions_sdf.write.mode("append").parquet("hdfs:///user/otus/card-fraud-detection/transactions.parquet")


if __name__ == '__main__':
    import sys

    conf = SparkConf().setAppName("Update dataset")
    sc = SparkContext(conf=conf)
    nb_days, start_date, radius = sys.argv[1:]

    update(
        sc, 
        int(nb_days), 
        dt.datetime.strptime(start_date, "%Y-%m-%d"), 
        int(radius),
    )
