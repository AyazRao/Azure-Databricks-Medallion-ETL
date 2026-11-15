# Databricks notebook source
# ----------------------------------------------------------
# Step 0: Initial Setup and Configuration
# ----------------------------------------------------------
# Import necessary PySpark functions at the beginning for clarity.
from pyspark.sql.functions import col, to_date, sum
import re

# Define all configuration variables in a single, easy-to-update block.
storage_account_name = "stdataengportfolio"
storage_account_key = "<STORAGE_ACCOUNT_KEY>"
bronze_path = f"abfss://lake@{storage_account_name}.dfs.core.windows.net/bronze/salesdata/ingestion_date=2025-09-01/"
silver_path = f"abfss://lake@{storage_account_name}.dfs.core.windows.net/silver/superstore_historical/"
gold_path = f"abfss://lake@{storage_account_name}.dfs.core.windows.net/gold/sales_aggregates/"

# Configure the Azure Data Lake storage key to allow Spark to access the Data Lake.
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# ----------------------------------------------------------
# Step 1: Read the Bronze Layer Data
# ----------------------------------------------------------
print("➡️ Reading historical data from Bronze layer...")
# Read the raw historical sales data from the Bronze layer using a CSV reader.
df_historical = spark.read.csv(bronze_path, header=True, inferSchema=True)
print("✅ Historical data read successfully.")

# ----------------------------------------------------------
# Step 2: Cleanse and Rename Columns
# ----------------------------------------------------------
print("➡️ Cleansing and transforming data...")
# Automatically clean up column names by removing invalid characters and converting to snake_case.
# This prevents the Delta Lake error and is a best practice for data warehousing.
def clean_column_name(column):
    # Remove special characters and replace spaces with underscores
    clean_name = re.sub(r'[^a-zA-Z0-9_]', '', column.replace(' ', '_'))
    return clean_name.lower()

# Apply the cleaning function to all column names.
renamed_df = df_historical
for column_name in df_historical.columns:
    new_name = clean_column_name(column_name)
    renamed_df = renamed_df.withColumnRenamed(column_name, new_name)

# ----------------------------------------------------------
# Step 3: Transform Data for the Silver Layer
# ----------------------------------------------------------
# Cleanse the DataFrame by converting date columns from string to a proper date type.
df_silver = renamed_df.withColumn(
    "order_date", to_date(col("order_date"), "M/d/yyyy")
).withColumn(
    "ship_date", to_date(col("ship_date"), "M/d/yyyy")
)

# Calculate new, derived columns ('total_sales' and 'total_profit') to enrich the dataset.
df_silver = df_silver.withColumn(
    "total_sales", col("sales") * col("quantity")
).withColumn(
    "total_profit", col("profit") * col("quantity")
)
print("✅ Data cleansed and transformed.")

# ----------------------------------------------------------
# Step 4: Write to the Silver Layer
# ----------------------------------------------------------
print(f"➡️ Writing transformed data to Silver layer at: {silver_path}")
# Write the cleaned and enriched DataFrame to the Silver layer in Delta format.
df_silver.write.format("delta").mode("overwrite").save(silver_path)
print("✅ Silver layer data successfully written.")

# Create an external Delta table for the Silver layer for easy SQL querying.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver_historical_sales
    USING DELTA
    LOCATION '{silver_path}'
""")
print("✅ Silver Delta table created.")