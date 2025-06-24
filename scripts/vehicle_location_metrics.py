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

# Join with location names
location_metrics = location_metrics.join(location_df.withColumnRenamed("location_id", "pickup_location"),
                                         on="pickup_location", how="left")

# --- KPI 2: Rental Duration & Revenue by Vehicle Type ---
# Join with vehicle type
vehicle_metrics = rental_df.join(vehicle_df.select("vehicle_id", "vehicle_type"), on="vehicle_id", how="left")

vehicle_type_metrics = vehicle_metrics.groupBy("vehicle_type").agg(
    sum("rental_duration_hours").alias("total_rental_hours"),
    sum("total_amount").alias("total_revenue")
)

# Save to S3 as Parquet
location_metrics.write.mode("overwrite").parquet("s3://rentalbk/processed/location_metrics/")
vehicle_type_metrics.write.mode("overwrite").parquet("s3://rentalbk/processed/vehicle_type_metrics/")