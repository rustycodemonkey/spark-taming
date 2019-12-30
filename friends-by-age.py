from pyspark.sql import SparkSession

# Use existing Databricks Connect session
# Note: Cannot modify existing SparkConf
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
# sc.setLogLevel("INFO")


def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)


lines = sc.textFile("s3a://mypersonaldumpingground/spark_taming_data/fakefriends.csv")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()

for result in results:
    print(result)



### Original Example ###
# from pyspark import SparkConf, SparkContext
#
# conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
# sc = SparkContext(conf = conf)
#
# def parseLine(line):
#     fields = line.split(',')
#     age = int(fields[2])
#     numFriends = int(fields[3])
#     return (age, numFriends)
#
# lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
# rdd = lines.map(parseLine)
# totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
# averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
# results = averagesByAge.collect()
# for result in results:
#     print(result)
