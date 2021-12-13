# Police Violence Project
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys

conf = SparkConf().setAppName('Causes of Death')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

spark.read.option('header', True).csv(sys.argv[1]) \
    .rdd.map(lambda x: (x[18], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False) \
    .saveAsTextFile(sys.argv[2])
