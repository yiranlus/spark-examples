# %%
# # Load other modules
from bisect import bisect_right
import re

from pyspark.sql import SparkSession

import os
os.chdir(os.path.dirname(__file__))

# %% Comprehensive English stopwords list (NLTK-style)
stopwords = [
    "i","me","my","myself","we","our","ours","ourselves",
    "you","your","yours","yourself","yourselves",
    "he","him","his","himself","she","her","hers","herself",
    "it","its","itself","they","them","their","theirs","themselves",
    "what","which","who","whom","this","that","these","those",
    "am","is","are","was","were","be","been","being",
    "have","has","had","having","do","does","did","doing",
    "a","an","the",
    "and","but","if","or","because","as","until","while",
    "of","at","by","for","with","about","against","between","into","through",
    "during","before","after","above","below","to","from","up","down","in","out",
    "on","off","over","under","again","further","then","once",
    "here","there","when","where","why","how",
    "all","any","both","each","few","more","most","other","some","such",
    "no","nor","not","only","own","same","so","than","too","very",
    "s","t","can","will","just","don","should","now",
    "d","ll","m","o","re","ve","y",
    "ain","aren","couldn","didn","doesn","hadn","hasn","haven","isn",
    "ma","mightn","mustn","needn","shan","shouldn","wasn","weren","won","wouldn"
]


# %% Initiliaze Spark Session
spark = (
    SparkSession.builder.master("local[*]").appName("beautiful_stories").getOrCreate()
)
sc = spark.sparkContext

# %%
beautiful_stories = sc.textFile("pg1430.txt").zipWithIndex()
# Show the first 10 Lines
# print("First 10 lines of the file:")
# for t, r in beautiful_stories.take(10):
#     print(f"{r:>3}: {t}")
# print()

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

for title, line_no in title_line_nos[:-1]:
    print(f"Title: {title:<30} Line No: {line_no}")
line_nos = list(map(lambda x: x[1], title_line_nos))
print()

# %% Extract Content of Each Story
def index_le(a, x):
    'Find the index of rightmost value less than x'
    i = bisect_right(a, x)
    if i:
        return i-1
    return -1


story_lines = beautiful_stories.groupBy(
    lambda x: index_le(line_nos, x[1])
).mapValues(
    lambda x: sorted(x, key=lambda x: x[1])
)

# %% Split Story to Files
os.makedirs("stories", exist_ok=True)
for i, lines in story_lines.collect():
    if i == -1 or i == len(line_nos)-1:
        continue

    with open(f"stories/{title_line_nos[i][0]}.txt", "w") as f:
        f.writelines(map(lambda x: x[0] + "\n", lines))


word_frequencies = (
    story_lines
    .flatMapValues(
        lambda lines: " ".join(map(lambda x: x[0], lines)).split()
    ).map(lambda kv: ((kv[0], re.sub(r"[.,“”!?]", "", kv[1].lower())), 1))
    .reduceByKey(lambda a, b: a + b)
    .map(lambda kv: (kv[0][0], (kv[0][1], kv[1])))
    .groupByKey()
    .mapValues(lambda x: sorted(x, key=lambda x: x[1], reverse=True))
).collect()

for i, wordlist in word_frequencies:
    if i == -1 or i == len(line_nos)-1:
        continue

    print(f"Title: {title_line_nos[i][0]:<30}")

    counter = 0
    for word, freq in wordlist:
        if word in stopwords:
            continue

        counter += 1
        print(f"{word:<20} {freq}")
        if counter == 10:
            break

    print()

# %%
sc.stop()
