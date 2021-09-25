# spark-basic.py
from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
spark = SparkSession.builder.master("local[*]")\
    .config("spark.jars", "./jars/spark-xml_2.12-0.6.0.jar")\
    .getOrCreate()
#import pandas as pd 

schema = StructType([
    StructField("1", StringType()),
    StructField("2", StringType()),
    StructField("3", StringType()),
    StructField("4", StringType()),
    StructField("5", StringType()),
    StructField("6", StringType()),
    StructField("7", StringType()),
    StructField("8", StringType()),
    StructField("9", StringType()),
    StructField("10", StringType()),
    StructField("11", StringType()),
    StructField("12", StringType()),
    StructField("13", StringType()),
    StructField("14", StringType()),
    StructField("15", StringType()),
    StructField("16", StringType()),
    StructField("17", StringType()),
    StructField("18", StringType()),
    StructField("19", StringType()),
    StructField("20", StringType()),
    StructField("21", StringType()),
    StructField("22", StringType()),
    StructField("23", StringType()),
    StructField("24", StringType()),
    StructField("25", StringType()),
    StructField("26", StringType()),
    StructField("27", StringType()),
    StructField("28", StringType()),
    StructField("29", StringType()),
    StructField("30", StringType()),
    StructField("31", StringType()),
    StructField("32", StringType()),
    StructField("33", StringType()),
    StructField("34", StringType()),
    StructField("35", StringType()),
    StructField("36", StringType()),
    StructField("37", StringType()),
    StructField("38", StringType()),
    StructField("39", StringType()),
    ])

dados = spark.read.options(delimiter="|", header=False).schema(schema).load('*.txt', format="csv")
data = dados.toPandas()

data['empresa_competencia'] = ''
data['NF'] = ''
nf = None
arquivo = 0
for empresa in data.index:
    if data['2'][empresa] == "0000":
        arquivo = arquivo + 1
        data['empresa_competencia'][empresa] = arquivo
        for i in data.index:
            if data['2'][i] == "C100":
                nf = data['10'][i]
                data['NF'][i] = nf
            else:
                data['NF'][i] = nf
    else:
        data['empresa_competencia'][empresa] = arquivo

dadosC100 = data[data['2'].isin(["C100"])]
dadosC170 = data[data['2'].isin(["C170"])]
dadosC190 = data[data['2'].isin(["C190"])]
dadosC197 = data[data['2'].isin(["C197"])]
dados0150 = data[data['2'].isin(["0150"])]
dados0200 = data[data['2'].isin(["0200"])]
dados0000 = data[data['2'].isin(["0000"])]
dadosE110 = data[data['2'].isin(["E110"])]
dadosE111 = data[data['2'].isin(["E111"])]

dadosC100['10'] = dadosC100['10'].astype(str)

dadosC100.to_csv('dadosC100.csv', sep=';', index=False)
dadosC170.to_csv('dadosC170.csv', sep=';', index=False)
dadosC190.to_csv('dadosC190.csv', sep=';', index=False)
dadosC197.to_csv('dadosC197.csv', sep=';', index=False)
dados0150.to_csv('dados0150.csv', sep=';', index=False)
dados0200.to_csv('dados0200.csv', sep=';', index=False)
dados0000.to_csv('dados0000.csv', sep=';', index=False)
dadosE110.to_csv('dadosE110.csv', sep=';', index=False)
dadosE111.to_csv('dadosE111.csv', sep=';', index=False)