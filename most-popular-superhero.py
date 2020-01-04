from pyspark.sql import SparkSession
import smart_open

# Use existing Databricks Connect session
# Note: Cannot modify existing SparkConf
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
# sc.setLogLevel("INFO")


def countCoOccurences(line):
    elements = line.split()
    return int(elements[0]), len(elements) - 1


def parseNames(line):
    fields = line.split('\"')
    return int(fields[0]), fields[1].encode("utf8")


names = sc.textFile("s3a://mypersonaldumpingground/spark_taming_data/Marvel-Names.txt")
namesRdd = names.map(parseNames)

# Super Hero ID, Name
'''
1 "24-HOUR MAN/EMMANUEL"
2 "3-D MAN/CHARLES CHAN"
3 "4-D MAN/MERCURIO"
4 "8-BALL/"
5 "A"
'''


lines = sc.textFile("s3a://mypersonaldumpingground/spark_taming_data/Marvel-Graph.txt")
pairings = lines.map(countCoOccurences)

# Super Hero, Other Marvel charaters
'''
5988 748 1722 3752 4655 5743 1872 3413 5527 6368 6085 4319 4728 1636 2397 3364 4001 1614 1819 1585 732 2660 3952 2507 3891 2070 2239 2602 612 1352 5447 4548 1596 5488 1605 5517 11 479 2554 2043 17 865 4292 6312 473 534 1479 6375 4456 
5989 4080 4264 4446 3779 2430 2297 6169 3530 3272 4282 6432 2548 4140 185 105 3878 2429 1334 4595 2767 3956 3877 4776 4946 3407 128 269 5775 5121 481 5516 4758 4053 1044 1602 3889 1535 6038 533 3986 
5982 217 595 1194 3308 2940 1815 794 1503 5197 859 5096 6039 2664 651 2244 528 284 1449 1097 1172 1092 108 3405 5204 387 4607 4545 3705 4930 1805 4712 4404 247 4754 4427 1845 536 5795 5978 533 3984 6056 
5983 1165 3836 4361 1282 716 4289 4646 6300 5084 2397 4454 1913 5861 5485 
5980 2731 3712 1587 6084 2472 2546 6313 875 859 323 2664 1469 522 2506 2919 2423 3624 5736 5046 1787 5776 3245 3840 2399
'''


totalFriendsByCharacter = pairings.reduceByKey(lambda x, y: x + y)
flipped = totalFriendsByCharacter.map(lambda xy: (xy[1], xy[0]))

mostPopular = flipped.max()
print(mostPopular)

print(namesRdd.lookup(mostPopular[1]))

mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print(str(mostPopularName, 'utf-8', 'ignore') + " is the most popular superhero, with " +
      str(mostPopular[0]) + " co-appearances.")


# Another way to get max value from pair rdd
# examplerdd = sc.parallelize([(1, 100), (2, 50), (3, 10)])
# print(examplerdd.max(lambda x: x[0]))

### Original Example ###
# from pyspark import SparkConf, SparkContext
#
# conf = SparkConf().setMaster("local").setAppName("PopularHero")
# sc = SparkContext(conf = conf)
#
# def countCoOccurences(line):
#     elements = line.split()
#     return (int(elements[0]), len(elements) - 1)
#
# def parseNames(line):
#     fields = line.split('\"')
#     return (int(fields[0]), fields[1].encode("utf8"))
#
# names = sc.textFile("file:///SparkCourse/marvel-names.txt")
# namesRdd = names.map(parseNames)
#
# lines = sc.textFile("file:///SparkCourse/marvel-graph.txt")
#
# pairings = lines.map(countCoOccurences)
# totalFriendsByCharacter = pairings.reduceByKey(lambda x, y : x + y)
# flipped = totalFriendsByCharacter.map(lambda xy : (xy[1], xy[0]))
#
# mostPopular = flipped.max()
#
# mostPopularName = namesRdd.lookup(mostPopular[1])[0]
#
# print(str(mostPopularName) + " is the most popular superhero, with " + \
#     str(mostPopular[0]) + " co-appearances.")
