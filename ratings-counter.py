from pyspark.sql import SparkSession
import collections

# Use existing Databricks Connect session
# Note: Cannot modify existing SparkConf
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
# sc.setLogLevel("INFO")

lines = sc.textFile("s3a://mypersonaldumpingground/ml-100k/u.data")
print(type(lines))
ratings = lines.map(lambda x: x.split()[2])
print(type(ratings))
print(ratings.take(5))

'''
User Id, Movie ID, Rating, Timestamp
196	242	3	881250949
186	302	3	891717742
22	377	1	878887116
244	51	2	880606923
166	346	1	886397596
'''

result = ratings.countByValue()
# result is collections.defaultdict
print(type(result))
print(sorted(result.items()))

sortedResults = collections.OrderedDict(sorted(result.items()))
print(type(sortedResults))
print(sortedResults)

for key, value in sortedResults.items():
    print("%s %i" % (key, value))


### Original Example ###
# from pyspark import SparkConf, SparkContext
# import collections
#
# conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
# sc = SparkContext(conf = conf)
#
# lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
# ratings = lines.map(lambda x: x.split()[2])
# result = ratings.countByValue()
#
# sortedResults = collections.OrderedDict(sorted(result.items()))
# for key, value in sortedResults.items():
#     print("%s %i" % (key, value))
