# %%
# # Load other modules
import matplotlib.pyplot as plt
import pyspark.sql.functions as sf
from pyspark.sql import SparkSession
from typing import Iterable

# %% Initiliaze Spark Session
spark = (
    SparkSession.builder.master("local[*]").appName("beautiful_stories").getOrCreate()
)
sc = spark.sparkContext

# %%
_beautiful_stories = sc.textFile("beautiful_stories/pg1430.txt")
beautiful_stories = _beautiful_stories.zipWithIndex()
# Show the first 10 Lines
print("First 10 lines of the file:")
for t, r in beautiful_stories.take(10):
    print(f"{r:>3}: {t}")
print()

# %% Retrieve Story Title in Content
row_start = 238
row_end = 257

content_titles = beautiful_stories.filter(lambda x: row_start <= x[1] <= row_end).map(
    lambda x: x[0].split(". ")[0].strip()
).collect()

print("All the titles:")
for i, title in enumerate(content_titles):
    print(f"Title {i + 1:>3}: {title}")

class ContentDict:
    def __init__(self):
        self.min_row_id: int = -1
        self.current_title: str = ""
        self.unknown_content: list[tuple[str, int]] = []
        self.known_content: dict[str, list[tuple[str, int]]] = {}
        
    
# %% Aggregate Text to Titles
def aggregate_text(d: ContentDict, row: tuple[str, int]) -> ContentDict:
    if d.min_row_id == -1:
        d.min_row_id = row[1]
    else:
       d.min_row_id = min(d.min_row_id, row[1])
       
    if row[0] in content_titles:
        d.current_title = row[0]
        d.known_content[row[0]] = []
        return d
    
    if d.current_title == "":
        d.unknown_content.append(row)
        return d
    
    d.known_content[d.current_title].append(row)
    return d
def aggregate_texts(d: ContentDict, rows: Iterable[tuple[str, int]]) -> Iterable[ContentDict]:
    for row in rows:
        d = aggregate_text(d, row)
    yield d

content_dicts = beautiful_stories.mapPartitions(
    lambda p: aggregate_texts(ContentDict(), p)
)

content_dicts.sortBy(lambda x: x.min_row_id).fold()

# %%
sc.stop()
