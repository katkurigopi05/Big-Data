from pyspark import SparkContext

def main():

   sc = SparkContext(appName='SparkWordCount')

   distFile = sc.textFile('/home/120student120/input.txt')
   print(distFile.glom().collect())
   
   sc.stop()

if __name__ == '__main__':
   main()