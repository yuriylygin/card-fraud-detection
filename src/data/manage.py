import findspark
findspark.init()

import pyspark

# import pyspark.pandas as ps
import datetime as dt
import click

from src.emulator import generate_dataset, add_frauds, generate_transactions_table

@click.command()
@click.option('--n-customers', type=int, default=5000)
@click.option('--n-terminals', type=int, default=10000)
@click.option('--nb-days', type=int, default=250)
@click.option('--start-date', type=click.DateTime(formats=["%Y-%m-%d"]), default=str(dt.date(year=2021, month=1, day=1)))
@click.option('--radius', type=int, default=5)
def create(n_customers=5000, n_terminals=10000, nb_days=30, start_date=dt.date(year=2022, month=1, day=1), radius=10):

    customer_profiles_table_df, terminal_profiles_table_df, transactions_df = \
        generate_dataset(
            n_customers=n_customers, 
            n_terminals=n_terminals, 
            nb_days=nb_days, 
            start_date=start_date, 
            r=radius,
        )

    transactions_df = add_frauds(customer_profiles_table_df, terminal_profiles_table_df, transactions_df)
        
    spark = (
        pyspark.sql.SparkSession.builder
            .appName("create")
            .master("yarn")
            .config("spark.executor.memory", "1g")
            .config("spark.driver.memory", "1g")
            .getOrCreate()
    )

    transactions_sdf=spark.createDataFrame(transactions_df)
    customer_profiles_table_sdf=spark.createDataFrame(customer_profiles_table_df)
    terminal_profiles_table_sdf=spark.createDataFrame(terminal_profiles_table_df)

    transactions_sdf.write.mode("append").parquet("hdfs:///user/otus/card-fraud-detection/transactions.parquet")
    customer_profiles_table_sdf.write.mode("append").parquet("hdfs:///user/otus/card-fraud-detection/customer_profiles_table.parquet")
    terminal_profiles_table_sdf.write.mode("append").parquet("hdfs:///user/otus/card-fraud-detection/terminal_profiles_table.parquet") 

    for day in range(nb_days - 1):
        transactions_df = (
            customer_profiles_table_df
                .groupby('CUSTOMER_ID').apply(lambda x : generate_transactions_table(x.iloc[0], start_date=start_date+dt.timedelta(days=day),  nb_days=1)).reset_index(drop=True)
        )
        transactions_df = add_frauds(customer_profiles_table_df, terminal_profiles_table_df, transactions_df)
        transactions_sdf=spark.createDataFrame(transactions_df)
        transactions_sdf.write.mode("append").parquet("hdfs:///user/otus/card-fraud-detection/transactions.parquet")

    spark.stop()

@click.command()
def delete():

    spark = (
        pyspark.sql.SparkSession.builder
            .appName("delete")
            .master("yarn")
            .config("spark.executor.memory", "1g")
            .config("spark.driver.memory", "1g")
            .getOrCreate()
    )
    sc = spark.sparkContext
    fs = (sc._jvm.org
          .apache.hadoop
          .fs.FileSystem
          .get(sc._jsc.hadoopConfiguration())
          )
    fs.delete(sc._jvm.org.apache.hadoop.fs.Path("/user/otus/card-fraud-detection"), True)

@click.command()
@click.option('--nb-days', type=int, default=250)
@click.option('--start-date', type=click.DateTime(formats=["%Y-%m-%d"]), default=str(dt.date(year=2021, month=1, day=1)))
@click.option('--radius', type=int, default=5)
def update(nb_days=30, start_date=dt.date(year=2022, month=2, day=1), radius=5):

    spark = (
        pyspark.sql.SparkSession.builder
            .appName("update")
            .master("yarn")
            .config("spark.executor.memory", "1g")
            .config("spark.driver.memory", "1g")
            .getOrCreate()
    )
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
    
    for day in range(nb_days):
        transactions_df = (
            customer_profiles_table_df
                .groupby('CUSTOMER_ID').apply(lambda x : generate_transactions_table(x.iloc[0], start_date=start_date+dt.timedelta(days=day),  nb_days=1)).reset_index(drop=True)
        )
        transactions_df = add_frauds(customer_profiles_table_df, terminal_profiles_table_df, transactions_df)
        transactions_sdf = spark.createDataFrame(transactions_df) 
        transactions_sdf.write.mode("append").parquet("hdfs:///user/otus/card-fraud-detection/transactions.parquet")

    spark.stop()


@click.group()
def cli():
    """
    CLI NTRNX ml service
    """


cli.add_command(create)
cli.add_command(delete)
cli.add_command(update)


if __name__ == '__main__':
    cli()

