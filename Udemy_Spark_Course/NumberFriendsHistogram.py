from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("NumberOfFriendsHistogram").getOrCreate()
sc = spark.sparkContext

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

# Load data
lines = sc.textFile("hdfs:///user/maria_dev/users/fakefriends.csv")

# Transform data to get rdd like (age, numFriends) 
rdd = lines.map(parseLine)
# Map values to get new rdd like (age, (numFriend, 1))
totalByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
# Map values to get new rdd like (age, avgNumFriends)
avgNumFriendsByAge = totalByAge.mapValues(lambda x: x[0] / x[1])
# Transform rdd to collect, then print out final result
results = avgNumFriendsByAge.collect()
for result in results:
    print(result)