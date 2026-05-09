from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordsCount").getOrCreate()
sc = spark.sparkContext

lines = sc.textFile("hdfs:///user/maria_dev/spark_practice/book.txt")

words = lines.flatMap(lambda x: x.split())
wordsList = words.countByValue()

for key, value in wordsList.items():
    word = key.encode('ascii', 'ignore')

    print("{0}: {1}".format(word, value))