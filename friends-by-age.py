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

# Input data
'''
0,Will,33,385
1,Jean-Luc,26,2
2,Hugh,55,221
3,Deanna,40,465
4,Quark,68,21
'''

rdd = lines.map(parseLine)
print(rdd.take(5))

totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
print(totalsByAge.take(5))

averagesByAge = totalsByAge.mapValues(lambda x: int(x[0] / x[1]))
print(averagesByAge.take(5))

results = averagesByAge.collect()
print(type(results))

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
