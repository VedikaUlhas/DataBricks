
#import sys, os
#os.environ["PYSPARK_PYTHON"] = sys.executable
#os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession 


spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Test") \
    .getOrCreate()

text_file = spark.sparkContext.textFile("sample.txt")

counts = (
    text_file.flatMap(lambda line: line.split(" "))
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)
)

print(counts.collect())