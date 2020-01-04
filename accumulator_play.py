from pyspark.sql import SparkSession

# Use existing Databricks Connect session
# Note: Cannot modify existing SparkConf
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
# sc.setLogLevel("INFO")

# from pyspark import SparkContext
# sc = SparkContext("local", "Accumulator app")
# sc.setLogLevel("WARN")
print(type(spark))
print(type(sc))
print(help(sc))


# num = sc.accumulator(0)
#
#
# def f(x):
#     # global num
#     num.add(x)
#
#
# rdd = sc.parallelize([1, 2, 3, 4]).foreach(f)
# print("Accumulated value is {}".format(num.value))
