# Databricks notebook source
# ----------------------------------------------------------
# Step 0: Initial Setup and Configuration
# ----------------------------------------------------------
# Import necessary PySpark functions.
from pyspark.sql.functions import col
import re

# Define all configuration variables in a single, easy-to-update block.
storage_account_name = "stdataengportfolio"
storage_account_key = "<STORAGE_ACCOUNT_KEY>"
bronze_api_path = f"abfss://lake@{storage_account_name}.dfs.core.windows.net/bronze/api/ingestion_date=2025-09-01/"
silver_api_path = f"abfss://lake@{storage_account_name}.dfs.core.windows.net/silver/superstore_api/"

# Configure the Azure Data Lake storage key to allow Spark to access the Data Lake.
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# ----------------------------------------------------------
# Step 1: Read the Bronze Layer Data
# ----------------------------------------------------------
print("➡️ Reading API data from Bronze layer...")
# Read the API data from the Bronze layer using a JSON reader.
# The files are JSON, so spark.read.json() is the correct method.
df_api = spark.read.json(bronze_api_path, multiLine=True)
print("✅ API data read successfully.")

# Print the columns to check for any inconsistencies.
print("Original columns:", df_api.columns)

# ----------------------------------------------------------
# Step 2: Transform and Curate Data for the Silver Layer
# ----------------------------------------------------------
print("➡️ Curating API data...")
# Select and rename relevant columns to create a clean, curated product catalog.
# This is a more direct and reliable way to handle inconsistent column names.
# We explicitly select the original columns and alias them to the desired names.
df_silver_api = df_api.select(
    col("Product ID").alias("product_id"),
    col("Product Name").alias("product_name"),
    col("Category").alias("category"),
    col("Sub-Category").alias("sub_category"),
    col("Sales").alias("price")
)
print("✅ API data curated.")

# ----------------------------------------------------------
# Step 3: Write to the Silver Layer
# ----------------------------------------------------------
print(f"➡️ Writing curated API data to Silver layer at: {silver_api_path}")
# Write the curated DataFrame to a separate Silver layer table in Delta format.
df_silver_api.write.format("delta").mode("overwrite").save(silver_api_path)
print("✅ API data successfully transformed into Silver layer.")

# Create an external Delta table for the Silver API data.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver_superstore_api
    USING DELTA
    LOCATION '{silver_api_path}'
""")
print("✅ Silver Delta table created for API data.")
