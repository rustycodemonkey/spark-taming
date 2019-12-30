from pyspark.sql import SparkSession

# Use existing Databricks Connect session
# Note: Cannot modify existing SparkConf
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
# sc.setLogLevel("INFO")


def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    # temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    temperature = float(fields[3]) * 0.1
    return (stationID, entryType, temperature)


lines = sc.textFile("s3a://mypersonaldumpingground/spark_taming_data/1800.csv")

# Weather stn ID, YYYYMMDD, TEMPMAX, -7.5 celcius ...
'''
ITE00100554,18000101,TMAX,-75,,,E,
ITE00100554,18000101,TMIN,-148,,,E,
GM000010962,18000101,PRCP,0,,,E,
EZE00100082,18000101,TMAX,-86,,,E,
EZE00100082,18000101,TMIN,-135,,,E,
ITE00100554,18000102,TMAX,-60,,I,E,
ITE00100554,18000102,TMIN,-125,,,E,
GM000010962,18000102,PRCP,0,,,E,
EZE00100082,18000102,TMAX,-44,,,E,
EZE00100082,18000102,TMIN,-130,,,E,
'''

parsedLines = lines.map(parseLine)
print(type(parsedLines))
print(parsedLines.take(5))

minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
print(type(minTemps))
print(minTemps.take(5))

stationTemps = minTemps.map(lambda x: (x[0], x[2]))
print(type(stationTemps))
print(stationTemps.take(5))

minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
print(type(minTemps))
print(minTemps.take(5))

results = minTemps.collect()

for result in results:
    print(result[0] + "\t{:.2f}C".format(result[1]))


### Original Example ###
# from pyspark import SparkConf, SparkContext
#
# conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
# sc = SparkContext(conf = conf)
#
# def parseLine(line):
#     fields = line.split(',')
#     stationID = fields[0]
#     entryType = fields[2]
#     temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
#     return (stationID, entryType, temperature)
#
# lines = sc.textFile("file:///SparkCourse/1800.csv")
# parsedLines = lines.map(parseLine)
# minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
# stationTemps = minTemps.map(lambda x: (x[0], x[2]))
# minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
# results = minTemps.collect();
#
# for result in results:
#     print(result[0] + "\t{:.2f}F".format(result[1]))
