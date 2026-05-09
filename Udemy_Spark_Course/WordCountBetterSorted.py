import re
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordsCount").getOrCreate()
sc = spark.sparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

lines = sc.textFile("hdfs:///user/maria_dev/spark_practice/book.txt")

words = lines.flatMap(normalizeWords)
# wordsList = words.countByValue()
wordsCount = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordsCountFlipped = wordsCount.map(lambda x: (x[1],x[0])).sortByKey()
results = wordsCountFlipped.collect()

for result in results:
    count = result[0]
    word = result[1]
    if word:
        print("{0} :\t\t {1}".format(word, count))