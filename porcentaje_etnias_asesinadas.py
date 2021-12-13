from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setAppName('deaths_arrests')
sc = SparkContext(conf = conf)

data = sc.textFile('deaths_arrests.csv')
header = data.first() #Obtengo la cabecera
data = data.filter(lambda row: row != header) #La elimino

def quitarCaracteres(line):
        newLine = []
        for word in line:
                newLine.append(re.sub('[",]','', word))
        return newLine

def calPor(PAsesinEt, PTotalEt):
        if (int(PAsesinEt) == 0 or int(PTotalEt) == 0):
                return 0
        else:
                return (int(PAsesinEt)/ int(PTotalEt)) * 100

data.map(lambda line: re.sub('(?<=,),',',0,',line))\
        .map(lambda line: re.findall('".+?",|.+?,', line))\
        .map(lambda line: quitarCaracteres(line))\
        .flatMap(lambda line: [('Black', calPor(line[3],line[12])),('Hispanic', calPor(line[4],line[20])),
                ('Native American', calPor(line[5],line[14])),('Asian',calPor(line[6],line[15])),
                ('Pacific Islanders',calPor(line[7],line[17])),('White', calPor(line[8],line[13]))])\
        .groupByKey()\
        .map(lambda por: (por[0], sum(list(por[1]))/len(list(por[1]))))\
        .saveAsTextFile("output.txt")