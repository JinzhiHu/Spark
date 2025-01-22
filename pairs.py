import re

from pyspark import SparkContext

cs = SparkContext()

lines = cs.textFile("Shakespeare.txt")

import itertools
pairs_count = lines.flatMap(lambda line: itertools.combinations(re.findall(r"[a-z]+(?:'[a-z]+)?", line.lower()), 2))\
                   .map(lambda pair: (pair, 1))\
                   .reduceByKey(lambda x, y: x + y)

processed = pairs_count.flatMap(lambda tu: [[tu[0], tu[1]]])
Returned_list = [["Pair", "Count"]] + list(processed.collect())


filename = "pairs.csv"
import csv
with open(filename, 'w') as f:
    writer = csv.writer(f)
    writer.writerows(Returned_list)