
# import pyspark.pandas as ps
import datetime as dt

from src.emulator import generate_dataset, add_frauds


def initiate(n_customers=5000, n_terminals=10000, nb_days=250, start_date=dt.date(year=2021, month=1, day=1), r=5):

    customer_profiles_table_df, terminal_profiles_table_df, transactions_df = \
        generate_dataset(
            n_customers=n_customers, 
            n_terminals=n_terminals, 
            nb_days=nb_days, 
            start_date=start_date, 
            r=r,
        )
    return customer_profiles_table_df, terminal_profiles_table_df, transactions_df


if __name__ == '__main__':
    import findspark
    findspark.init()

    import pyspark

    spark = (
        pyspark.sql.SparkSession.builder
            .appName("initiate")
            .master("yarn")
            .config("spark.executor.memory", "1g")
            .config("spark.driver.memory", "1g")
            .getOrCreate()
    )

    customer_profiles_table_df, terminal_profiles_table_df, transactions_df = initiate()

    transactions_sdf=spark.createDataFrame(transactions_df) 
    transactions_sdf.write.mode("append").parquet("hdfs:///user/otus/card-fraud-detection/transactions.parquet")

    customer_profiles_table_sdf=spark.createDataFrame(customer_profiles_table_df) 
    customer_profiles_table_sdf.write.mode("append").parquet("hdfs:///user/otus/card-fraud-detection/customer_profiles_table.parquet")

    terminal_profiles_table_sdf=spark.createDataFrame(terminal_profiles_table_df) 
    terminal_profiles_table_sdf.write.mode("append").parquet("hdfs:///user/otus/card-fraud-detection/terminal_profiles_table.parquet") 

    spark.stop()
   
