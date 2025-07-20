from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, datediff, hour, round, when, floor, date_trunc, current_date
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

# Read silver tables
donations = spark.read.parquet(f"{silver_path}/donations")
projects = spark.read.parquet(f"{silver_path}/projects")
volunteer_shifts = spark.read.parquet(f"{silver_path}/volunteer_shifts")
campaigns = spark.read.parquet(f"{silver_path}/campaigns")
volunteers = spark.read.parquet(f"{silver_path}/volunteers")
beneficiaries = spark.read.parquet(f"{silver_path}/beneficiaries")
transactions = spark.read.parquet(f"{silver_path}/transactions")

# Aggregation 1: Total donations per project
donations_per_project = donations.join(projects, "project_id") \
    .groupBy("project_id", "project_name") \
    .agg(
        sum("amount").alias("total_donations"),
        count("*").alias("donation_count")
    )

# Aggregation 2: Total volunteer hours per project
volunteer_hours = volunteer_shifts.join(projects, "project_id") \
    .groupBy("project_id", "project_name") \
    .agg(sum("shift_duration_hours").alias("total_hours"))

# Aggregation 3: Donations by region
donations_by_region = donations.join(projects, "project_id") \
    .groupBy("region") \
    .agg(
        sum("amount").alias("total_donations"),
        count("*").alias("donation_count")
    )

# Aggregation 3: Donations by region
donations_by_region = donations.join(projects, "project_id") \
    .groupBy("region") \
    .agg(
        sum("amount").alias("total_donations"),
        count("*").alias("donation_count")
    )

#Aggregation 4: Campaign performance
campaign_performance = donations.join(campaigns, "campaign_id") \
    .groupBy("campaign_id", "title", "target_amount") \
    .agg(
        sum("amount").alias("total_donations"),
        count("*").alias("donation_count")
    ) \
    .withColumn("percent_target_achieved", round((col("total_donations") / col("target_amount")) * 100, 2))

# Aggregation 5: Donor activity
donor_activity = donations.groupBy("donor_id", "donor_name") \
    .agg(
        sum("amount").alias("total_donations"),
        count("*").alias("donation_count")
    ) \
    .orderBy(col("total_donations").desc())
# Aggregation 6: Volunteer engagement by age group
volunteer_engagement = volunteer_shifts.join(volunteers, "volunteer_id") \
    .withColumn("age_group", 
        when(col("age").between(16, 25), "16-25")
        .when(col("age").between(26, 35), "26-35")
        .when(col("age").between(36, 45), "36-45")
        .when(col("age").between(46, 55), "46-55")
        .when(col("age").between(56, 65), "56-65")
        .when(col("age").between(66, 80), "66-80")
        .otherwise("Unknown")
    ) \
    .groupBy("age_group") \
    .agg(
        sum("shift_duration_hours").alias("total_hours"),
        count("*").alias("shift_count")
    )
# Aggregation 7: Beneficiary demographics by aid type
beneficiary_demographics = beneficiaries.groupBy("aid_type") \
    .agg(
        count("*").alias("beneficiary_count"),
        round(avg("age"), 2).alias("average_age")
    )
# Aggregation 8: Transaction success rate
transaction_success_rate = transactions.groupBy("payment_provider") \
    .agg(
        count("*").alias("total_transactions"),
        sum(when(col("status") == "Success", 1).otherwise(0)).alias("successful_transactions")
    ) \
    .withColumn("success_rate", round((col("successful_transactions") / col("total_transactions")) * 100, 2))
# Aggregation 9: Donation trends over time
donation_trends = donations.groupBy(date_trunc("month", "donation_date").alias("month")) \
    .agg(
        sum("amount").alias("total_donations"),
        count("*").alias("donation_count")
    ) \
    .orderBy("month")
#Aggregation 10: Active campaigns by region
active_campaigns = campaigns.join(projects, "project_id", "left") \
    .filter(col("end_date") >= current_date()) \
    .groupBy("region") \
    .agg(count("*").alias("active_campaign_count"))



# Write to gold layer
donations_per_project.write.mode("overwrite").parquet(f"{gold_path}/donations_per_project")
volunteer_hours.write.mode("overwrite").parquet(f"{gold_path}/volunteer_hours_per_project")
donations_by_region.write.mode("overwrite").parquet(f"{gold_path}/donations_by_region")
campaign_performance.write.mode("overwrite").parquet(f"{gold_path}/campaign_performance")
donor_activity.write.mode("overwrite").parquet(f"{gold_path}/donor_activity")
volunteer_engagement.write.mode("overwrite").parquet(f"{gold_path}/volunteer_engagement")
beneficiary_demographics.write.mode("overwrite").parquet(f"{gold_path}/beneficiary_demographics")
transaction_success_rate.write.mode("overwrite").parquet(f"{gold_path}/transaction_success_rate")
donation_trends.write.mode("overwrite").parquet(f"{gold_path}/donation_trends")
active_campaigns.write.mode("overwrite").parquet(f"{gold_path}/active_campaigns")

print("Silver to Gold processing completed.")
spark.stop()
