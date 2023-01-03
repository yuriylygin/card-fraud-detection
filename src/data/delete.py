from pyspark import SparkConf, SparkContext


def delete(sc):
    fs = (sc._jvm.org
          .apache.hadoop
          .fs.FileSystem
          .get(sc._jsc.hadoopConfiguration())
          )
    fs.delete(sc._jvm.org.apache.hadoop.fs.Path("/user/otus/card-fraud-detection"), True)


if __name__ == '__main__':
    conf = SparkConf().setAppName("Delete dataset")
    sc = SparkContext(conf=conf)
    delete(sc)
