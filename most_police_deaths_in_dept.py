# Police Violence Project
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys

conf = SparkConf().setAppName('Department With Most Deaths in Department')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

result = spark.read.option('header', True).csv(sys.argv[1]) \
    .rdd.map(lambda x: (x[1], 1)) \
    .filter(lambda x: "TX" in x[0]) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False) \
    .first()

print(result)
