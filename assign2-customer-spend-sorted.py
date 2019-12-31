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
cust_spend = parsed_lines.reduceByKey(lambda x, y: x + y)
cust_spend_sorted = cust_spend.map(lambda x: (x[1], x[0])).sortByKey().collect()

for total_spend, customer in cust_spend_sorted:
    print('{}: {:.2f}'.format(customer, total_spend))
