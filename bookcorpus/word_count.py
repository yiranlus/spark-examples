"""
A simple PySpark task that creates an RDD, performs a transformation,
and collects the results.
"""
import os
from string import punctuation

from pyspark.sql import SparkSession

data_dir = "/home/yiranls/downloads/bookcorpus"

with open(f"{data_dir}/stopwords.txt") as f:
    stopwords = [word.strip() for word in f.readlines()]

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("simple_task")
    .getOrCreate()
)
sc = spark.sparkContext

text = sc.textFile(",".join([f"{data_dir}/{file}" for file in os.listdir(data_dir)]))

word_frequencies = (
    text
    .flatMap(
        lambda line: line.lower().split()
    ).map(
        lambda word: word.strip(punctuation)
    ).filter(
        lambda word: word not in stopwords and word not in punctuation
    ).countByValue()
)

top20_words = sorted(word_frequencies.items(), key=lambda x: x[1], reverse=True)[:20]

for word, count in top20_words:
    print(f"{word:<20}: {count}")

sc.stop()
