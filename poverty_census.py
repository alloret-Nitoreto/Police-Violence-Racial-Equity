from pyspark import SparkConf, SparkContext
import re
import sys
from time import time 

conf = SparkConf().setAppName('poverty_census')
sc = SparkContext(conf = conf)

data = sc.textFile('poverty_census_bureau.csv')
header = data.first() #Obtengo la cabecera
data = data.filter(lambda row: row != header) #La elimino


data.map(lambda line: re.split('","', line)) \
    .map(lambda line: (line[1], line[2])) \
    .filter(lambda line: line[1] != 'null') \
    .filter(lambda line: line[0] != "United States") \
    .map(lambda line: (line[0].split(',')[1], line[1])) \
    .map(lambda line: (line[0].split()[0],int(line[1]))) \
    .reduceByKey(lambda a, b: int(a) + int(b)) \
    .saveAsTextFile("poverty_data")

