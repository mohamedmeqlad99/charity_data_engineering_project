from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp
from dotenv import load_dotenv
import os

load_dotenv()
AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")
AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")

if not AZURE_CONN_STR or not CONTAINER_NAME or not AZURE_STORAGE_ACCOUNT_NAME:
    raise ValueError("Missing environment variables. Check .env file.")


spark = SparkSession.builder \
    .appName("BronzeToSilver") \
    .config("spark.hadoop.fs.azure.account.auth.type", "SAS") \
    .config("spark.hadoop.fs.azure.sas.token.provider.type", "org.apache.hadoop.fs.azure.SimpleSasTokenProvider") \
    .config("spark.hadoop.fs.azure.sas.fixed.token", AZURE_CONN_STR) \
    .config("spark.databricks.delta.eventGrid.enabled", "true") \
    .getOrCreate()

blob_base_path = f"wasbs://{CONTAINER_NAME}@{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
bronze_path = f"{blob_base_path}/bronze"
silver_path = f"{blob_base_path}/silver"
checkpoint_path = f"{blob_base_path}/checkpoints/bronze_to_silver"


tables = ["projects", "campaigns", "donations", "volunteers", "volunteer_shifts", "beneficiaries", "transactions"]

for table in tables:

    df = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/{table}/schema") \
        .option("header", "true") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .option("cloudFiles.useNotifications", "true") \
        .load(f"{bronze_path}/{table}")

    # Clean and transform
    if table == "campaigns":
        df = df.withColumn("start_date", to_date(col("start_date"))) \
               .withColumn("end_date", to_date(col("end_date"))) \
               .withColumn("target_amount", col("target_amount").cast("double"))
    elif table == "donations":
        df = df.withColumn("amount", col("amount").cast("double")) \
               .withColumn("donation_date", to_date(col("donation_date")))
    elif table == "volunteers":
        df = df.withColumn("age", col("age").cast("integer")) \
               .withColumn("join_date", to_date(col("join_date")))
    elif table == "volunteer_shifts":
        df = df.withColumn("date", to_date(col("date"))) \
               .withColumn("start_time", to_timestamp(col("start_time"), "HH:mm:ss")) \
               .withColumn("end_time", to_timestamp(col("end_time"), "HH:mm:ss"))
    elif table == "beneficiaries":
        df = df.withColumn("age", col("age").cast("integer"))
    elif table == "transactions":
        df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

    # Remove duplicates based on ID columns
    id_column = f"{table[:-1]}_id" if table != "projects" else "project_id"
    if id_column in df.columns:
        df = df.dropDuplicates([id_column])


    df.writeStream \
        .format("parquet") \
        .option("checkpointLocation", f"{checkpoint_path}/{table}/checkpoint") \
        .outputMode("append") \
        .start(f"{silver_path}/{table}")

print("Bronze to Silver streaming job started with file notification mode.")
spark.streams.awaitAnyTermination()