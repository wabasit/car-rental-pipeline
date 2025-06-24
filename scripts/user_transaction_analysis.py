from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, min, to_date, unix_timestamp

spark = SparkSession.builder.appName("UserTransactionAnalysis").getOrCreate()

# Read datasets
rental_df = spark.read.csv("s3://rentalbk/raw_data/rental_transactions.csv", header=True, inferSchema=True)

