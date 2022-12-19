import sys
from pyspark import SparkConf, SparkContext

from src.data.manage import update


def main():
    conf = SparkConf().setAppName("Update dataset - PySpark")
    sc = SparkContext(conf=conf)
    
    print("-------------======= UPDATE =======-------------")


if __name__ == "__main__":
    main()
