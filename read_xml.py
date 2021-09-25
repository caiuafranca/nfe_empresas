# spark-basic.py
from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
spark = SparkSession.builder.master("local[*]")\
    .config("spark.jars", "./jars/spark-xml_2.12-0.6.0.jar")\
    .getOrCreate()


rootTag = "catalog"
rowTag = "book"

df = spark.read.format('xml').options(rootTag=rootTag).options(rowTag=rowTag)\
    .load("./XML/books.xml")

df.printSchema()