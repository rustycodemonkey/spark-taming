from pyspark.sql import SparkSession
import re


# Use existing Databricks Connect session
# Note: Cannot modify existing SparkConf
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
# sc.setLogLevel("INFO")

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

input = sc.textFile("s3a://mypersonaldumpingground/spark_taming_data/Book.txt")
words = input.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
# Flip Key Value and then use sortByKey
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)


### Original Example ###
# import re
# from pyspark import SparkConf, SparkContext
#
# def normalizeWords(text):
#     return re.compile(r'\W+', re.UNICODE).split(text.lower())
#
# conf = SparkConf().setMaster("local").setAppName("WordCount")
# sc = SparkContext(conf = conf)
#
# input = sc.textFile("file:///sparkcourse/book.txt")
# words = input.flatMap(normalizeWords)
#
# wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
# wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
# results = wordCountsSorted.collect()
#
# for result in results:
#     count = str(result[0])
#     word = result[1].encode('ascii', 'ignore')
#     if (word):
#         print(word.decode() + ":\t\t" + count)
