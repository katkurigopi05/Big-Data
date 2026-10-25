import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

spark = SparkSession.builder.master("local").appName("hdfs_test").getOrCreate()

booksSchema = StructType() \
                    	.add("Title", "string")\
                    	.add("Author", "string")\
                    	.add("Genre", "string")\
                    	.add("Height", "integer")\
				.add("Publisher","string")
                        
booksdata=spark.read.csv("hdfs:///home/120student120/GoodBooks.csv",header='true',schema=booksSchema)
booksdata.show(5, truncate = False)

booksdata.write.csv("hdfs:///home/120student120/output2")
booksdata.printSchema()
