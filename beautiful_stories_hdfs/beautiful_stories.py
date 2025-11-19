# %%
# # Load other modules
from string import punctuation
from bisect import bisect_right

from pyspark.sql import SparkSession

data_root = "hdfs://51.91.85.24:9000/data"

# %% Initiliaze Spark Session
spark = (
    SparkSession.builder
    .appName("beautiful_stories")
    .getOrCreate()
)
sc = spark.sparkContext

# Set Log Level
sc.setLogLevel("WARN")

# %% Comprehensive English stopwords list (NLTK-style)
stopwords = sc.textFile(f"{data_root}/stopwords.txt").collect()

# %% Load pg1430.txt with Line Number
beautiful_stories = sc.textFile(f"{data_root}/beautiful_stories/pg1430.txt").zipWithIndex()

# %% Show the first 10 Lines
#print("First 10 lines of the file:")
#for t, r in beautiful_stories.take(10):
#    print(f"{r:>3}: {t}")
#print()

# %% Retrieve Story Title in Content
content_row_start = 238
content_row_end = 257

content_titles = (
    beautiful_stories
    .filter(lambda x: content_row_start <= x[1] <= content_row_end + 1)
    .map(lambda x: x[0].split(". ")[0].strip())
    .collect()
)

# Ill-formated Title
for i in range(len(content_titles)):
    if content_titles[i] == "PRONOUNCING VOCABULARY OF NAMES":
        content_titles[i] = content_titles[i] + "."

# %% Find the line number of the first line of each story
title_line_nos = (
    beautiful_stories
    .filter(lambda x: x[0] in content_titles)
    .sortBy(lambda x: x[1])
).collect()
title_line_nos[-1] = (title_line_nos[-1][0] + ".", title_line_nos[-1][1])

# for title, line_no in title_line_nos[:-1]:
#    print(f"Title: {title:<30} Line No: {line_no}")
# print()
line_nos = list(map(lambda x: x[1], title_line_nos))

# %% Extract Content of Each Story
def index_le(a, x):
    'Find the index of rightmost value less than x'
    i = bisect_right(a, x)
    if i:
        return i-1
    return -1

story_lines = beautiful_stories.groupBy(
    # Group by Story Title
    lambda x: index_le(line_nos, x[1])
).mapValues(
    lambda x: sorted(x, key=lambda x: x[1])
)

# %% Split Story to Files
for i, lines in story_lines.collect():
    if i == -1 or i == len(line_nos)-1:
        continue

    print(f"Saving stories/{title_line_nos[i][0]}.txt")
    try:
        (
            # Preserve Line Order by Loading into Only One Partition
            sc.parallelize(lines, numSlices=1)
            .map(lambda x: x[0])
            .saveAsTextFile(f"{data_root}/beautiful_stories/stories/{title_line_nos[i][0]}.txt")
        )
    except Exception as e:
        if "FileAlreadyExistsException" not in str(e):
            raise(e)
print()

# %% Extract Word Frequencies
word_frequencies = (
    story_lines
    .flatMapValues(
        lambda lines: " ".join(map(lambda x: x[0], lines)).split()
    ).map(lambda kv: ((kv[0], kv[1].lower().strip(punctuation)), 1))
    .reduceByKey(lambda a, b: a + b)
    .map(lambda kv: (kv[0][0], (kv[0][1], kv[1])))
    .groupByKey()
    .mapValues(lambda x: sorted(x, key=lambda x: x[1], reverse=True))
).collect()

# %% Save Word Frequencies to Files
for i, wordlist in word_frequencies:
    if i == -1 or i == len(line_nos)-1:
        continue

    print(f"Saving word_frequencies/{title_line_nos[i][0]}.txt")
    try:
        (
            # Preserve Line Order by Loading into Only One Partition
            sc.parallelize(wordlist, numSlices=1)
            .map(lambda wordfreq: f"{wordfreq[0]:<20} {wordfreq[1]}")
            .saveAsTextFile(f"{data_root}/beautiful_stories/word_frequencies/{title_line_nos[i][0]}.txt")
        )
    except Exception as e:
        if "FileAlreadyExistsException" not in str(e):
            raise(e)

# %% Stop Spark Context
sc.stop()
