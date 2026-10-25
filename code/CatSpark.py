import pyspark 
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("hdfs_test").getOrCreate()                       
inputdata=spark.read.csv("hdfs:///home/120student120/quangle.txt")
inputdata.show(truncate = False)
