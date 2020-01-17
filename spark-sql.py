from pyspark.sql import SparkSession
from pyspark.sql import Row
import collections

# Use existing Databricks Connect session
spark = SparkSession.builder.appName("XXXTESTXXX").getOrCreate()


def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))


'''
0,Will,33,385
1,Jean-Luc,26,2
2,Hugh,55,221
3,Deanna,40,465
4,Quark,68,21
'''

lines = spark.sparkContext.textFile("s3a://mypersonaldumpingground/spark_taming_data/fakefriends.csv")
people = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
    print(teen)

# We can also use functions instead of SQL queries:
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()

### Original Example ###
# from pyspark.sql import SparkSession
# from pyspark.sql import Row
#
# import collections
#
# # Create a SparkSession (Note, the config section is only for Windows!)
# spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("SparkSQL").getOrCreate()
#
# def mapper(line):
#     fields = line.split(',')
#     return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))
#
# lines = spark.sparkContext.textFile("fakefriends.csv")
# people = lines.map(mapper)
#
# # Infer the schema, and register the DataFrame as a table.
# schemaPeople = spark.createDataFrame(people).cache()
# schemaPeople.createOrReplaceTempView("people")
#
# # SQL can be run over DataFrames that have been registered as a table.
# teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
#
# # The results of SQL queries are RDDs and support all the normal RDD operations.
# for teen in teenagers.collect():
#   print(teen)
#
# # We can also use functions instead of SQL queries:
# schemaPeople.groupBy("age").count().orderBy("age").show()
#
# spark.stop()
