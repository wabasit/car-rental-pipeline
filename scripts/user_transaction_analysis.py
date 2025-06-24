from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, min, to_date, unix_timestamp

spark = SparkSession.builder.appName("UserTransactionAnalysis").getOrCreate()

# Read datasets
rental_df = spark.read.csv("s3://rentalbk/raw_data/rental_transactions.csv", header=True, inferSchema=True)

# Rental duration in hours
rental_df = rental_df.withColumn("rental_duration_hours",
    (unix_timestamp("rental_end_time") - unix_timestamp("rental_start_time")) / 3600
)

rental_df = rental_df.withColumn("rental_date", to_date("rental_start_time"))

# --- KPI 1: Daily Metrics ---
daily_metrics = rental_df.groupBy("rental_date").agg(
    count("*").alias("daily_transactions"),
    sum("total_amount").alias("daily_revenue")
)

# --- KPI 2: User Metrics ---
user_metrics = rental_df.groupBy("user_id").agg(
    count("*").alias("total_transactions"),
    sum("total_amount").alias("total_spent"),
    avg("total_amount").alias("avg_transaction"),
    max("total_amount").alias("max_transaction"),
    min("total_amount").alias("min_transaction"),
    sum("rental_duration_hours").alias("total_rental_hours")
)


