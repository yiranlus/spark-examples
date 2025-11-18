"""
A simple PySpark task that creates an RDD, performs a transformation,
and collects the results.
"""
from pyspark import SparkConf, SparkContext

conf = (
    SparkConf()
    .setMaster("spark://51.91.85.76:7077")
    .setAppName("simple_task")
)
sc = SparkContext(conf=conf)

l = sc.parallelize(range(10))
l_double = l.map(lambda x: x * 2)

print(l_double.collect())

sc.stop()
