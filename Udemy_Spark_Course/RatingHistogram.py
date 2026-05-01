# from pyspark import SparkContext
# from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import collections

# Way 1 to create spark session
# spark = SparkSession.builder.master("local").appName("RatingHistogram").getOrCreate()
# sc = spark.sparkContext

# Way 2 to do the same thing for creation of spark session
conf = SparkConf().setMaster("local").setAppName("RatingHistogram")
sc = SparkContext(conf=conf)

movies = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

ratings = movies.map(lambda x: x.split()[2]) # Get rating per movie
countRatings = ratings.countByValue()

result = collections.OrderedDict(sorted(countRatings.items()))

for k, v in result.iteritems():
    print("%s: %i" % (k, v))