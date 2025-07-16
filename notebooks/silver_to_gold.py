from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, datediff, hour
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")

if not AZURE_CONN_STR or not CONTAINER_NAME or not AZURE_STORAGE_ACCOUNT_NAME:
    raise ValueError("Missing environment variables. Check .env file.")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SilverToGold") \
    .config("spark.hadoop.fs.azure.account.auth.type", "SAS") \
    .config("spark.hadoop.fs.azure.sas.token.provider.type", "org.apache.hadoop.fs.azure.SimpleSasTokenProvider") \
    .config("spark.hadoop.fs.azure.sas.fixed.token", AZURE_CONN_STR) \
    .getOrCreate()

# Define paths
blob_base_path = f"wasbs://{CONTAINER_NAME}@{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
silver_path = f"{blob_base_path}/silver"
gold_path = f"{blob_base_path}/gold"

# Read silver data
donations = spark.read.parquet(f"{silver_path}/donations")
projects = spark.read.parquet(f"{silver_path}/projects")
volunteer_shifts = spark.read.parquet(f"{silver_path}/volunteer_shifts")

# Process data to create gold layer
# Calculate total donations and count per project
donations_per_project = donations.join(projects, "project_id") \
    .groupBy("project_id", "project_name") \
    .agg(
        sum("amount").alias("total_donations"),
        count("*").alias("donation_count")
    )

# Calculate total volunteer hours per project
volunteer_hours = volunteer_shifts.join(projects, "project_id") \
    .withColumn("hours", (datediff(col("end_time"), col("start_time")) * 24 + hour(col("end_time")) - hour(col("start_time")))) \
    .groupBy("project_id", "project_name") \
    .agg(sum("hours").alias("total_hours"))

# Write processed data to gold layer
donations_per_project.write.mode("overwrite").parquet(f"{gold_path}/donations_per_project")
volunteer_hours.write.mode("overwrite").parquet(f"{gold_path}/volunteer_hours_per_project")

print("Silver to Gold processing completed.")
spark.stop()