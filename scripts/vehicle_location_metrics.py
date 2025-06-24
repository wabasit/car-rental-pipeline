from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum, avg, max, min, unix_timestamp

spark = SparkSession.builder.appName("VehicleLocationMetrics").getOrCreate()

