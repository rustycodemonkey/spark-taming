from pyspark.sql import SparkSession

# Use existing Databricks Connect session
# Note: Cannot modify existing SparkConf
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
# sc.setLogLevel("INFO")

lines = sc.textFile("s3a://mypersonaldumpingground/spark_taming_data/ml-100k/u.data")

'''
User Id, Movie ID, Rating, Timestamp
196	242	3	881250949
186	302	3	891717742
22	377	1	878887116
244	51	2	880606923
166	346	1	886397596
'''

# Which movie ID appears most frequently

movies = lines.map(lambda x: (int(x.split()[1]), 1))
print(movies.take(5))

movieCounts = movies.reduceByKey(lambda x, y: x + y)
print(movieCounts.take(5))

flipped = movieCounts.map(lambda x: (x[1], x[0]))
sortedMovies = flipped.sortByKey()

results = sortedMovies.collect()

for result in results:
    print(result)


### Original Example ###
# from pyspark import SparkConf, SparkContext
#
# conf = SparkConf().setMaster("local").setAppName("PopularMovies")
# sc = SparkContext(conf = conf)
#
# lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
# movies = lines.map(lambda x: (int(x.split()[1]), 1))
# movieCounts = movies.reduceByKey(lambda x, y: x + y)
#
# flipped = movieCounts.map( lambda xy: (xy[1],xy[0]) )
# sortedMovies = flipped.sortByKey()
#
# results = sortedMovies.collect()
#
# for result in results:
#     print(result)
