# Police Violence Project
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys
from time import time

conf = SparkConf().setAppName('City Murder Count')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

t_inicial = time()

spark.read.option('header', True).csv(sys.argv[1]) \
    .rdd.map(lambda x: (x[10], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False) \
    .saveAsTextFile(sys.argv[2])

print(time() - t_inicial)
