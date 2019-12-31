from pyspark.sql import SparkSession

# Use existing Databricks Connect session
# Note: Cannot modify existing SparkConf
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
# sc.setLogLevel("INFO")

input = sc.textFile("s3a://mypersonaldumpingground/spark_taming_data/customer-orders.csv")


def parse_lines(line):
    return int(line.split(',')[0]), float(line.split(',')[2])


parsed_lines = input.map(parse_lines)
cust_spend = parsed_lines.reduceByKey(lambda x, y: x + y).collect()

for customer, total_spend in sorted(cust_spend):
    print('{}: {:.2f}'.format(customer, total_spend))
