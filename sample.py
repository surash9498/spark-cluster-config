from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("MySparkApplication") \
    .master("spark://localhost:7077") \
    .getOrCreate()

rdd1=spark.sparkContext.textFile("file:///bigdata/general.txt")
print(rdd1.collect())
