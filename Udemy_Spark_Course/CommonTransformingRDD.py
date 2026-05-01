from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import lit

if __name__ == "__main__":
    # Create a SparkSession (the config bit is only for Windows!)
    spark = SparkSession.builder.appName("SparkPractice").getOrCreate()

    # Get SparkContext for RDD object
    sc = spark.sparkContext

    data1 = sc.parallelize([1, 2, 3, 3])
    data2 = sc.parallelize([3, 4, 5])

    doubled_filtered = data1.map(lambda x: x * 10).filter(lambda x: x > 15)
    unique_data = data1.distinct()
    all_items = data1.union(data2)
    common_items = data1.intersection(data2)

    print("Map and filter: \n")
    for line in doubled_filtered.collect():
        print(line, ",")
    # print(f'Distinct: {unique_data.collect()}')
    # print(f'Union: {all_items.collect()}')
    # print(f'Intersection: {common_items.collect()}')