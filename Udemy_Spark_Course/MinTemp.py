from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("MinimumTemperature").getOrCreate()
sc = spark.sparkContext

def parseLine(line):
    fields = line.split(',')
    stationId = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationId, entryType, temperature)

lines = sc.textFile("hdfs:///user/maria_dev/1800.csv")

parsedLines = lines.map(parseLine)
MinTemperature = parsedLines.filter(lambda x: "TMIN" in x[1])
StationMinTemperature = MinTemperature.map(lambda x: (x[0], x[2]))
Results = StationMinTemperature.reduceByKey(lambda x, y: min(x, y))

printResults = Results.collect()
for result in printResults:
    print(result[0] + "\t{:.2f}F".format(result[1]))