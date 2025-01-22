import re

from pyspark import SparkContext

cs = SparkContext()

lines = cs.textFile("Shakespeare.txt")

counts = lines.flatMap(lambda line: re.findall(r"[a-z]+(?:'[a-z]+)?", line.lower()))\
              .map(lambda word: (word, 1))\
              .reduceByKey(lambda x, y: x + y)

#counts.saveAsTextFile("Result.txt")

processed = counts.flatMap(lambda tu: [[tu[0], tu[1]]])

Returned_list = list(processed.collect())

if __name__ == "main":
    print(Returned_list)

filename = "word_count.csv"
import csv
with open(filename, 'w') as f:
    writer = csv.writer(f)
    writer.writerows(Returned_list)
