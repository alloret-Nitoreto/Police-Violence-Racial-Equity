# Police Violence Project
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys


def toCSVLine(data):
    return ','.join(str(d) for d in data)


conf = SparkConf().setAppName('Police Deaths By Department')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

spark.read.option('header', True).csv(sys.argv[1]) \
    .rdd.map(lambda x: (x[1], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False) \
    .map(toCSVLine) \
    .saveAsTextFile(sys.argv[2])
