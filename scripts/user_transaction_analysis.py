from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, min, to_date, unix_timestamp
from pyspark.sql.utils import AnalysisException

def main():
    """
    Main function to compute user-level and daily transaction metrics 
    from rental transaction data using PySpark on EMR.
    """
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("UserTransactionAnalysis").getOrCreate()

        # Read the raw rental transaction data from S3
        rental_df = spark.read.csv(
            "s3://rentalbk/raw_data/rental_transactions.csv", 
            header=True, 
            inferSchema=True
        )

        # Calculate rental duration in hours
        rental_df = rental_df.withColumn(
            "rental_duration_hours",
            (unix_timestamp("rental_end_time") - unix_timestamp("rental_start_time")) / 3600
        )

        # Extract rental date from rental start time
        rental_df = rental_df.withColumn("rental_date", to_date("rental_start_time"))

        # --- KPI 1: Daily Revenue and Transactions ---
        daily_metrics = rental_df.groupBy("rental_date").agg(
            count("*").alias("daily_transactions"),
            sum("total_amount").alias("daily_revenue")
        )

        # --- KPI 2: User-specific Spending and Duration ---
        user_metrics = rental_df.groupBy("user_id").agg(
            count("*").alias("total_transactions"),
            sum("total_amount").alias("total_spent"),
            avg("total_amount").alias("avg_transaction"),
            max("total_amount").alias("max_transaction"),
            min("total_amount").alias("min_transaction"),
            sum("rental_duration_hours").alias("total_rental_hours")
        )

        # Save the daily metrics to S3 in Parquet format
        daily_metrics.write.mode("overwrite").parquet("s3://rentalbk/processed/daily_metrics/")

        # Save the user metrics to S3 in Parquet format
        user_metrics.write.mode("overwrite").parquet("s3://rentalbk/processed/user_metrics/")

        print("ETL job completed successfully.")

    except AnalysisException as ae:
        print(f"Analysis Exception occurred while processing: {ae}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        spark.stop()  # Gracefully stop the Spark session

if __name__ == "__main__":
    main()