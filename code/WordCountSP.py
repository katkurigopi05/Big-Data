from pyspark import SparkContext

def main():

   sc = SparkContext(appName='SparkWordCount')

   input_file = sc.textFile('hdfs:///home/120student120/quangle.txt')
   counts = input_file.flatMap(lambda line: line.split()) \
                     .map(lambda word: (word, 1)) \
                     .reduceByKey(lambda a, b: a + b)
   counts.saveAsTextFile('hdfs:///home/120student120/output15')

   sc.stop()

if __name__ == '__main__':
   main()