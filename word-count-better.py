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
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))


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
# wordCounts = words.countByValue()
#
# for word, count in wordCounts.items():
#     cleanWord = word.encode('ascii', 'ignore')
#     if (cleanWord):
#         print(cleanWord.decode() + " " + str(count))
