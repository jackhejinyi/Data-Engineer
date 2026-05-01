from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("MinimumTemperature").getOrCreate()
sc = spark.sparkContext

def parsedLine(line):
    fieldes = line.split(',')
    StationId = fieldes[0]
    EntryType = fieldes[2]
    Temp = float(fieldes[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (StationId, EntryType, Temp)

lines = sc.textFile("hdfs:///user/maria_dev/1800.csv")

parsedLines = lines.map(parsedLine)
MaxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
# Create a new RDD to store (stationId, Temp)
StationTemps = MaxTemps.map(lambda x: (x[0], x[2]))
Results = StationTemps.reduceByKey(lambda x, y: max(x, y))

PrintedResults = Results.collect()
for result in PrintedResults:
    print(result[0] + "\t{:.2f}F".format(result[1]))
