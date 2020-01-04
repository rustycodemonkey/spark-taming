from pyspark import SparkContext
sc = SparkContext("local", "Accumulator app")
sc.setLogLevel("WARN")
num = sc.accumulator(0)

def f(x):
    # global num
    num.add(x)

rdd = sc.parallelize([1, 2, 3, 4]).foreach(f)
print("Accumulated value is {}".format(num.value))
