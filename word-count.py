from pyspark.sql import SparkSession

# Use existing Databricks Connect session
# Note: Cannot modify existing SparkConf
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
# sc.setLogLevel("INFO")

input = sc.textFile("s3a://mypersonaldumpingground/spark_taming_data/Book.txt")
print(input.take(5))
words = input.flatMap(lambda x: x.split())
print(words.take(5))

wordCounts = words.countByValue()
print(type(wordCounts))

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))

### Original Example ###
# from pyspark import SparkConf, SparkContext
#
# conf = SparkConf().setMaster("local").setAppName("WordCount")
# sc = SparkContext(conf = conf)
#
# input = sc.textFile("file:///sparkcourse/book.txt")
# words = input.flatMap(lambda x: x.split())
# wordCounts = words.countByValue()
#
# for word, count in wordCounts.items():
#     cleanWord = word.encode('ascii', 'ignore')
#     if (cleanWord):
#         print(cleanWord.decode() + " " + str(count))
