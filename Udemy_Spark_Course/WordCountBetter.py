import re
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordsCount").getOrCreate()
sc = spark.sparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

lines = sc.textFile("hdfs:///user/maria_dev/spark_practice/book.txt")

words = lines.flatMap(normalizeWords)
wordsList = words.countByValue()

for key, value in wordsList.items():
    word = key.encode('ascii', 'ignore')

    print("{0}: {1}".format(word, value))