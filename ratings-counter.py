from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import collections

conf = SparkConf().setAppName("RatingsHistogram")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

sc = spark.sparkContext
# sc.setLogLevel("INFO")

lines = sc.textFile("s3a://mypersonaldumpingground/ml-100k/u.data")

ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
