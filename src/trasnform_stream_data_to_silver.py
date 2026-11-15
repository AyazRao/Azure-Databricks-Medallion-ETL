# Databricks notebook source
# ----------------------------------------------------------
# Step 0: Initial Setup and Configuration
# ----------------------------------------------------------
# Import necessary PySpark functions and types.
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType
import re
import time

# Define all configuration variables in a single, easy-to-update block.
storage_account_name = "stdataengportfolio"
storage_account_key = "<STORAGE_ACCOUNT_KEY>"
bronze_stream_path = f"abfss://lake@{storage_account_name}.dfs.core.windows.net/bronze/stream/superstore/"
silver_stream_path = f"abfss://lake@{storage_account_name}.dfs.core.windows.net/silver/superstore_stream/"

# Configure the Azure Data Lake storage key.
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# ----------------------------------------------------------
# Step 1: Define the Schema for Streaming Data
# ----------------------------------------------------------
# It is a best practice to define a schema for streaming data to prevent schema drift issues.
stream_schema = StructType([
    StructField("Row ID", LongType(), True),
    StructField("Order ID", StringType(), True),
    StructField("Order Date", StringType(), True),
    StructField("Ship Date", StringType(), True),
    StructField("Ship Mode", StringType(), True),
    StructField("Customer ID", StringType(), True),
    StructField("Customer Name", StringType(), True),
    StructField("Segment", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Postal Code", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Product ID", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Sub-Category", StringType(), True),
    StructField("Product Name", StringType(), True),
    StructField("Sales", DoubleType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Profit", DoubleType(), True)
])

# ----------------------------------------------------------
# Step 2: Read the Streaming Data and Transform
# ----------------------------------------------------------
print("➡️ Reading streaming data from Bronze layer...")
# Read the streaming data from the Bronze layer using the defined schema.
# The 'multiLine=True' option is required for multi-line JSON files.
df_stream = spark.readStream.format("json").schema(stream_schema).option("multiLine", "true").load(bronze_stream_path)

# Apply basic transformations to clean and prepare the data.
parsed_clean = df_stream.withColumn(
    "Order Date", to_date(col("Order Date"), "M/d/yyyy")
).withColumn(
    "Ship Date", to_date(col("Ship Date"), "M/d/yyyy")
)

# ----------------------------------------------------------
# Step 3: Clean Column Names for Delta Lake
# ----------------------------------------------------------
print("➡️ Cleaning column names for Delta Lake...")
# Create a function to clean column names by replacing spaces and special characters with underscores.
def clean_col_name(column_name):
    # Replace spaces and hyphens with underscores, and convert to lowercase.
    return re.sub("[^a-zA-Z0-9_]", "", column_name.replace(" ", "_").replace("-", "_")).lower()

# Rename all columns in the DataFrame using the cleaning function.
df_stream_cleaned = parsed_clean.select(
    [col(c).alias(clean_col_name(c)) for c in parsed_clean.columns]
)
print("✅ Column names cleaned.")

# ----------------------------------------------------------
# Step 4: Write the Clean Stream to Silver as Delta
# ----------------------------------------------------------
print(f"➡️ Writing clean stream data to Silver layer at: {silver_stream_path}")
# Write the clean stream to the Silver layer in Delta format.
query = (df_stream_cleaned.writeStream
      .outputMode("append")
      .format("delta")
      .option("checkpointLocation", f"{silver_stream_path}_checkpoint")
      .start(silver_stream_path))

# Use a more reliable method to wait for the stream to process and then stop it.
print("⏳ Waiting for stream to process initial files...")
time.sleep(10) # Wait for initial batch to be processed
# Wait up to 30 seconds for the query to finish its work
start_time = time.time()
while query.isActive and (time.time() - start_time < 30):
    time.sleep(5)
    if not query.lastProgress:
        # No progress means no new files are arriving. Stop the stream.
        break
query.stop()
print("✅ Streaming data written to Silver layer and stream stopped.")

# Create an external Delta table for the Silver stream for querying.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver_superstore_stream
    USING DELTA
    LOCATION '{silver_stream_path}'
""")
print("✅ Silver Delta table for stream created.")
