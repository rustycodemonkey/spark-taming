import sys
from pyspark.sql import SparkSession
from pyspark.mllib.recommendation import ALS, Rating
import smart_open


def loadMovieNames():
    movieNames = {}
    with smart_open.open("s3a://mypersonaldumpingground/spark_taming_data/ml-100k/u.item", encoding='iso8859-1') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


# Use existing Databricks Connect session
# Note: Cannot modify existing SparkConf
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
# sc.setLogLevel("INFO")

# Databricks needs absolute checkpoint directory
sc.setCheckpointDir('/checkpoint')

print("\nLoading movie names...")
nameDict = loadMovieNames()

data = sc.textFile("s3a://mypersonaldumpingground/spark_taming_data/ml-100k/u.data")

# ratings = data.map(lambda l: l.split()).map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2]))).cache()
# Databricks limitations with df.cache()
ratings = data.map(lambda l: l.split()).map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

# Build the recommendation model using Alternating Least Squares
print("\nTraining recommendation model...")
rank = 10
# Lowered numIterations to ensure it works on lower-end systems
numIterations = 10
model = ALS.train(ratings, rank, numIterations)

userID = int(sys.argv[1])

print("\nRatings for user ID " + str(userID) + ":")
userRatings = ratings.filter(lambda l: l[0] == userID)
for rating in userRatings.collect():
    print(nameDict[int(rating[1])] + ": " + str(rating[2]))

print("\nTop 10 recommendations:")
recommendations = model.recommendProducts(userID, 10)
for recommendation in recommendations:
    print(nameDict[int(recommendation[1])] + \
          " score " + str(recommendation[2]))

### Original Example ###
# import sys
# from pyspark import SparkConf, SparkContext
# from pyspark.mllib.recommendation import ALS, Rating
#
# def loadMovieNames():
#     movieNames = {}
#     with open("ml-100k/u.ITEM", encoding='ascii', errors="ignore") as f:
#         for line in f:
#             fields = line.split('|')
#             movieNames[int(fields[0])] = fields[1]
#     return movieNames
#
# conf = SparkConf().setMaster("local[*]").setAppName("MovieRecommendationsALS")
# sc = SparkContext(conf = conf)
# sc.setCheckpointDir('checkpoint')
#
# print("\nLoading movie names...")
# nameDict = loadMovieNames()
#
# data = sc.textFile("file:///SparkCourse/ml-100k/u.data")
#
# ratings = data.map(lambda l: l.split()).map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2]))).cache()
#
# # Build the recommendation model using Alternating Least Squares
# print("\nTraining recommendation model...")
# rank = 10
# # Lowered numIterations to ensure it works on lower-end systems
# numIterations = 6
# model = ALS.train(ratings, rank, numIterations)
#
# userID = int(sys.argv[1])
#
# print("\nRatings for user ID " + str(userID) + ":")
# userRatings = ratings.filter(lambda l: l[0] == userID)
# for rating in userRatings.collect():
#     print (nameDict[int(rating[1])] + ": " + str(rating[2]))
#
# print("\nTop 10 recommendations:")
# recommendations = model.recommendProducts(userID, 10)
# for recommendation in recommendations:
#     print (nameDict[int(recommendation[1])] + \
#         " score " + str(recommendation[2]))
