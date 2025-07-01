from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum, avg, max, min, unix_timestamp
from pyspark.sql.utils import AnalysisException

def main():
    """
    Main function to calculate:
    1. Revenue and transactions per pickup location
    2. Rental duration and revenue per vehicle type
    using Spark on AWS EMR.
    """
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("VehicleLocationMetrics").getOrCreate()

        # Load raw datasets from S3
        rental_df = spark.read.csv("s3://rentalbk/raw_data/rental_transactions.csv", header=True, inferSchema=True)
        location_df = spark.read.csv("s3://rentalbk/raw_data/locations.csv", header=True, inferSchema=True)
        vehicle_df = spark.read.csv("s3://rentalbk/raw_data/vehicles.csv", header=True, inferSchema=True)

        # Compute rental duration in hours
        rental_df = rental_df.withColumn(
            "rental_duration_hours",
            (unix_timestamp("rental_end_time") - unix_timestamp("rental_start_time")) / 3600
        )

        # --- KPI 1: Location-Based Metrics ---
        location_metrics = rental_df.groupBy("pickup_location").agg(
            count("*").alias("total_transactions"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_transaction"),
            max("total_amount").alias("max_transaction"),
            min("total_amount").alias("min_transaction"),
            countDistinct("vehicle_id").alias("unique_vehicles_used")
        )

        # Join with location information (name, city, etc.)
        location_metrics = location_metrics.join(
            location_df.withColumnRenamed("location_id", "pickup_location"),
            on="pickup_location",
            how="left"
        )

        # --- KPI 2: Vehicle Type Metrics ---
        # Join with vehicle metadata to get vehicle_type
        vehicle_metrics = rental_df.join(
            vehicle_df.select("vehicle_id", "vehicle_type"),
            on="vehicle_id",
            how="left"
        )

        vehicle_type_metrics = vehicle_metrics.groupBy("vehicle_type").agg(
            sum("rental_duration_hours").alias("total_rental_hours"),
            sum("total_amount").alias("total_revenue")
        )

        # Save results to S3 in Parquet format
        location_metrics.write.mode("overwrite").parquet("s3://rentalbk/processed/location_metrics/")
        vehicle_type_metrics.write.mode("overwrite").parquet("s3://rentalbk/processed/vehicle_type_metrics/")

        print("Vehicle and location metrics ETL completed successfully.")

    except AnalysisException as ae:
        print(f"Analysis Exception occurred: {ae}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        spark.stop()  # Ensure the Spark session is closed properly

if __name__ == "__main__":
    main()
