from pyspark import SparkConf, SparkContext
import re
import sys
from time import time 

conf = SparkConf().setAppName('education_census')
sc = SparkContext(conf = conf)

data = sc.textFile('education_census_bureau.csv')
header = data.first() #Obtengo la cabecera
data = data.filter(lambda row: row != header) #La elimino

#Calculamos la media de estudiantes racializados que han terminado estudios universitarios
data.map(lambda line: re.split('","', line)) \
    .map(lambda line: (line[1], line[335], line[337])) \
    .filter(lambda line: line[1] != 'null') \
    .filter(lambda line: line[2] != 'null' ) \
    .filter(lambda line: line[2] != '-')\
    .map(lambda line: (line[0].split(',')[1], line[1])) \
    .map(lambda line: (line[0].split()[0],line[1])) \
    .reduceByKey(lambda a, b: int(a) + int(b)) \
    .saveAsTextFile("education_data")

