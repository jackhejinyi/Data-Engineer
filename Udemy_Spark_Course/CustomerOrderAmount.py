from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CustomOrders").getOrCreate()
sc = spark.sparkContext

def normalizeOrders(line):
    fileds = line.split(',')
    customerID = int(fileds[0])
    orderAmount = float(fileds[2])
    return (customerID, orderAmount)


data = sc.textFile("hdfs:///user/maria_dev/spark_practice/customer-orders.csv")

# Convert data with (customerID, orderAmount)
lines = data.map(normalizeOrders)
# Sum up order amount by customerID
orders = lines.reduceByKey(lambda x, y: x + y)
# Flip data from (CustomerID, totalAmount) to (totalAmount, CustomerID)
# and sort it
ordersFlipped = orders.map(lambda x: (x[1], x[0])).sortByKey()

results = ordersFlipped.collect()

for result in results:
    print("{0}: {1}".format(result[1], result[0]))