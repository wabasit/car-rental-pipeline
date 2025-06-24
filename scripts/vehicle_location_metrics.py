from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum, avg, max, min, unix_timestamp

spark = SparkSession.builder.appName("VehicleLocationMetrics").getOrCreate()

# Read CSVs from S3
rental_df = spark.read.csv("s3://rentalbk/raw_data/rental_transactions.csv", header=True, inferSchema=True)
location_df = spark.read.csv("s3://rentalbk/raw_data/locations.csv", header=True, inferSchema=True)
vehicle_df = spark.read.csv("s3://rentalbk/raw_data/vehicles.csv", header=True, inferSchema=True)

# Rental duration (in hours)
rental_df = rental_df.withColumn("rental_duration_hours",
    (unix_timestamp("rental_end_time") - unix_timestamp("rental_start_time")) / 3600
)

# --- KPI 1: Revenue & Transactions per Location ---
location_metrics = rental_df.groupBy("pickup_location").agg(
    count("*").alias("total_transactions"),
    sum("total_amount").alias("total_revenue"),
    avg("total_amount").alias("avg_transaction"),
    max("total_amount").alias("max_transaction"),
    min("total_amount").alias("min_transaction"),
    countDistinct("vehicle_id").alias("unique_vehicles_used")
)