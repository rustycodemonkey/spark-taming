from pyspark.sql import SparkSession
import smart_open

# Use existing Databricks Connect session
# Note: Cannot modify existing SparkConf
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
# sc.setLogLevel("INFO")


def loadMovieNames():
    movieNames = {}
    with smart_open.open("s3a://mypersonaldumpingground/spark_taming_data/ml-100k/u.item", encoding='iso8859-1') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# u.item file
'''
1|Toy Story (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)|0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0
2|GoldenEye (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?GoldenEye%20(1995)|0|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0|1|0|0
3|Four Rooms (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Four%20Rooms%20(1995)|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|1|0|0
4|Get Shorty (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Get%20Shorty%20(1995)|0|1|0|0|0|1|0|0|1|0|0|0|0|0|0|0|0|0|0
5|Copycat (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Copycat%20(1995)|0|0|0|0|0|0|1|0|1|0|0|0|0|0|0|0|1|0|0
'''

nameDict = sc.broadcast(loadMovieNames())
print(type(nameDict))

lines = sc.textFile("s3a://mypersonaldumpingground/spark_taming_data/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map(lambda x: (x[1], x[0]))
sortedMovies = flipped.sortByKey()

sortedMoviesWithNames = sortedMovies.map(lambda countMovie: (nameDict.value[countMovie[1]], countMovie[0]))

results = sortedMoviesWithNames.collect()

for result in results:
    print(result)


### Original Example ###
# from pyspark import SparkConf, SparkContext
#
# def loadMovieNames():
#     movieNames = {}
#     with open("ml-100k/u.ITEM") as f:
#         for line in f:
#             fields = line.split('|')
#             movieNames[int(fields[0])] = fields[1]
#     return movieNames
#
# conf = SparkConf().setMaster("local").setAppName("PopularMovies")
# sc = SparkContext(conf = conf)
#
# nameDict = sc.broadcast(loadMovieNames())
#
# lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
# movies = lines.map(lambda x: (int(x.split()[1]), 1))
# movieCounts = movies.reduceByKey(lambda x, y: x + y)
#
# flipped = movieCounts.map( lambda x : (x[1], x[0]))
# sortedMovies = flipped.sortByKey()
#
# sortedMoviesWithNames = sortedMovies.map(lambda countMovie : (nameDict.value[countMovie[1]], countMovie[0]))
#
# results = sortedMoviesWithNames.collect()
#
# for result in results:
#     print (result)
