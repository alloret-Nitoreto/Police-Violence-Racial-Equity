from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setAppName('deaths_arrests')
sc = SparkContext(conf = conf)

data = sc.textFile('deaths_arrests.csv')
header = data.first() #Obtengo la cabecera
data = data.filter(lambda row: row != header) #La elimino

def meterCero(line):
        for i in range(len(line)):
                if line[i] == '':
                        line[i] = '0'
        return line

data.map(lambda line: line.split(','))\
        .map(lambda line: meterCero(line))\
        .flatMap(lambda line: [('Black', line[3]),('Hispanic', line[4]),
                        ('Native American', line[5]),('Asian',line[6]),
                        ('Pacific Islanders', line[7]),('White', line[8])])\
        .reduceByKey(lambda x, y: int(x) + int(y))\
        .saveAsTextFile("numeroMuertesTotalPorEtnia.csv")