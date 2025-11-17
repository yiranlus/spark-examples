"""
A simple PySpark task that creates an RDD, performs a transformation,
and collects the results.
"""
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("simple_task")
    # .setMaster("local[*]")
    .master("spark://51.91.85.24:7077")
    # .set("spark.executor.cores", "1")
    # .set("spark.executor.memory", "512m")
    .getOrCreate()
)
sc = spark.sparkContext

l = sc.parallelize(range(10))
l_double = l.map(lambda x: x * 2)

print(l_double.collect())

sc.stop()