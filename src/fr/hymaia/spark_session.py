from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("global spark session") \
    .master("local[*]") \
    .getOrCreate()
