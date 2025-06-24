from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum, avg, max, min, unix_timestamp

spark = SparkSession.builder.appName("VehicleLocationMetrics").getOrCreate()

# Read CSVs from S3
rental_df = spark.read.csv("s3://rentalbk/raw_data/rental_transactions.csv", header=True, inferSchema=True)
location_df = spark.read.csv("s3://rentalbk/raw_data/locations.csv", header=True, inferSchema=True)
vehicle_df = spark.read.csv("s3://rentalbk/raw_data/vehicles.csv", header=True, inferSchema=True)