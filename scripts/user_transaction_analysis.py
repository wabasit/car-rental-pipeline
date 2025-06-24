from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, min, to_date, unix_timestamp

spark = SparkSession.builder.appName("UserTransactionAnalysis").getOrCreate()


