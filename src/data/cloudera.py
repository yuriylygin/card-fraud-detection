from pyspark import SparkConf, SparkContext
 
    
if __name__ == "__main__":
    sc = SparkContext(appName="Simple App")
    import numpy as np
    sc.parallelize(range(1,10)).map(lambda x : np.__version__).collect()
