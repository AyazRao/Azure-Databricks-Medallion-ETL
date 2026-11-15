# Databricks notebook source
# ----------------------------------------------------------
# Batch Pipeline: Bronze → Silver → Gold
# ----------------------------------------------------------

from pyspark.sql.functions import col, to_date, sum, count
import re

# ----------------------------------------------------------
# Step 0: Initial Setup and Configuration
# ----------------------------------------------------------
storage_account_name = "stdataengportfolio"
storage_account_key = "<STORAGE_ACCOUNT_KEY>"

bronze_path = f"abfss://lake@{storage_account_name}.dfs.core.windows.net/bronze/salesdata/ingestion_date=2025-09-01/"
silver_path = f"abfss://lake@{storage_account_name}.dfs.core.windows.net/silver/superstore_historical/"
gold_path   = f"abfss://lake@{storage_account_name}.dfs.core.windows.net/gold/sales_aggregates/"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# ----------------------------------------------------------
# Step 1: Read the Bronze Layer Data
# ----------------------------------------------------------
print("➡️ Reading Bronze layer data...")
df_bronze = spark.read.csv(bronze_path, header=True, inferSchema=True)
print("✅ Bronze data loaded.")

# ----------------------------------------------------------
# Step 2: Clean & Standardize Columns
# ----------------------------------------------------------
print("➡️ Cleaning column names...")

def clean_column_name(column):
    clean_name = re.sub(r'[^a-zA-Z0-9_]', '', column.replace(' ', '_'))
    return clean_name.lower()

df_cleaned = df_bronze
for c in df_bronze.columns:
    df_cleaned = df_cleaned.withColumnRenamed(c, clean_column_name(c))

# Standardize again in case of duplicates
df_cleaned = df_cleaned.toDF(*[c.strip().lower().replace(" ", "_") for c in df_cleaned.columns])

print("✅ Columns cleaned.")

# ----------------------------------------------------------
# Step 3: Transform → Silver Layer
# ----------------------------------------------------------
print("➡️ Transforming data for Silver layer...")

df_silver = (
    df_cleaned
    .withColumn("order_date", to_date(col("order_date"), "M/d/yyyy"))
    .withColumn("ship_date", to_date(col("ship_date"), "M/d/yyyy"))
    .withColumn("total_sales", col("sales") * col("quantity"))
    .withColumn("total_profit", col("profit") * col("quantity"))
)

print("✅ Transformation complete.")

# ----------------------------------------------------------
# Step 4: Write to Silver Layer
# ----------------------------------------------------------
print(f"➡️ Writing to Silver: {silver_path}")
df_silver.write.format("delta").mode("overwrite").save(silver_path)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver_historical_sales
    USING DELTA
    LOCATION '{silver_path}'
""")
print("✅ Silver table ready.")

# ----------------------------------------------------------
# Step 5: Gold Aggregation
# ----------------------------------------------------------
print("➡️ Aggregating data for Gold layer...")

df_gold = (
    df_silver.groupBy("order_date", "category", "subcategory")
    .agg(
        sum("total_sales").alias("daily_sales"),
        sum("total_profit").alias("daily_profit"),
        count("order_id").alias("orders_count")
    )
    .orderBy("order_date")
)

print("✅ Aggregation complete.")

# ----------------------------------------------------------
# Step 6: Write to Gold Layer
# ----------------------------------------------------------
print(f"➡️ Writing to Gold: {gold_path}")
df_gold.write.format("delta").mode("overwrite").save(gold_path)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold_sales_aggregates
    USING DELTA
    LOCATION '{gold_path}'
""")
print("✅ Gold table ready.")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date, SUM(total_sales) as revenue
# MAGIC FROM gold_sales_aggregates
# MAGIC GROUP BY date
# MAGIC ORDER BY date DESC;