from pyspark.sql import SparkSession
from pyspark.sql import Row
import smart_open
from pyspark.sql import functions


def loadMovieNames():
    movieNames = {}
    with smart_open.open("s3a://mypersonaldumpingground/spark_taming_data/ml-100k/u.item", encoding='iso8859-1') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames



# Use existing Databricks Connect session
spark = SparkSession.builder.getOrCreate()

# Load up our movie ID -> name dictionary
nameDict = loadMovieNames()

# Get the raw data
lines = spark.sparkContext.textFile("s3a://mypersonaldumpingground/spark_taming_data/ml-100k/u.data")

'''
User Id, Movie ID, Rating, Timestamp
196	242	3	881250949
186	302	3	891717742
22	377	1	878887116
244	51	2	880606923
166	346	1	886397596
'''

# Convert it to a RDD of Row objects
movies = lines.map(lambda x: Row(movieID=int(x.split()[1])))
# Convert that to a DataFrame
movieDataset = spark.createDataFrame(movies)

# Some SQL-style magic to sort all movies by popularity in one line!
# topMovieIDs = movieDataset.groupBy("movieID").count().orderBy("count", ascending=False).cache()
# Databricks limitations with df.cache()
topMovieIDs = movieDataset.groupBy("movieID").count().orderBy("count", ascending=False)

# Show the results at this point:

# |movieID|count|
# +-------+-----+
# |     50|  584|
# |    258|  509|
# |    100|  508|

topMovieIDs.show()

# Grab the top 10
top10 = topMovieIDs.take(10)

# Print the results
print("\n")
for result in top10:
    # Each row has movieID, count as above.
    print("%s: %d" % (nameDict[result[0]], result[1]))

# Stop the session
spark.stop()

### Original Example ###
# from pyspark.sql import SparkSession
# from pyspark.sql import Row
# from pyspark.sql import functions
#
# def loadMovieNames():
#     movieNames = {}
#     with open("ml-100k/u.ITEM") as f:
#         for line in f:
#             fields = line.split('|')
#             movieNames[int(fields[0])] = fields[1]
#     return movieNames
#
# # Create a SparkSession (the config bit is only for Windows!)
# spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("PopularMovies").getOrCreate()
#
# # Load up our movie ID -> name dictionary
# nameDict = loadMovieNames()
#
# # Get the raw data
# lines = spark.sparkContext.textFile("file:///SparkCourse/ml-100k/u.data")
# # Convert it to a RDD of Row objects
# movies = lines.map(lambda x: Row(movieID =int(x.split()[1])))
# # Convert that to a DataFrame
# movieDataset = spark.createDataFrame(movies)
#
# # Some SQL-style magic to sort all movies by popularity in one line!
# topMovieIDs = movieDataset.groupBy("movieID").count().orderBy("count", ascending=False).cache()
#
# # Show the results at this point:
#
# #|movieID|count|
# #+-------+-----+
# #|     50|  584|
# #|    258|  509|
# #|    100|  508|
#
# topMovieIDs.show()
#
# # Grab the top 10
# top10 = topMovieIDs.take(10)
#
# # Print the results
# print("\n")
# for result in top10:
#     # Each row has movieID, count as above.
#     print("%s: %d" % (nameDict[result[0]], result[1]))
#
# # Stop the session
# spark.stop()
