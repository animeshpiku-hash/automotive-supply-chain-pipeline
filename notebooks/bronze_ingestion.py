# Databricks notebook source
# Cell 1: Mount Configuration

# 👉 REPLACE THESE VALUES
storage_account_name = "adlssupplychainam"
storage_account_key = "YOUR_ACCESS_KEY"

# Container names
bronze_container = "bronze"
silver_container = "silver"
gold_container = "gold"

# Configuration dictionary
configs = {
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net": storage_account_key
}

print(f"✓ Configuration set for storage account: {storage_account_name}")

# COMMAND ----------

# Cell 2: Configure direct ABFS access (no mounts needed)
# This works on all cluster security modes including USER_ISOLATION

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# Define base paths for each layer — use these instead of /mnt/ paths
bronze_path = f"abfss://{bronze_container}@{storage_account_name}.dfs.core.windows.net/"
silver_path = f"abfss://{silver_container}@{storage_account_name}.dfs.core.windows.net/"
gold_path   = f"abfss://{gold_container}@{storage_account_name}.dfs.core.windows.net/"

# Quick validation
dbutils.fs.ls(bronze_path)
print(f"✓ Connected to ADLS. Bronze path: {bronze_path}")

# COMMAND ----------

display(dbutils.fs.ls(bronze_path))

# COMMAND ----------

# Cell 3: Verify Mount and List Files
# Let's see what's in our bronze container

print("📂 Files in bronze container:")
files = dbutils.fs.ls(bronze_path)

if len(files) == 0:
    print("⚠️  No files found - did you upload the CSV?")
else:
    for file in files:
        print(f"  - {file.name} ({file.size} bytes)")

# COMMAND ----------

# Cell 4: Read CSV from Bronze Container
# Load the raw supplier delivery data

from pyspark.sql.functions import current_timestamp, input_file_name, lit

# Read CSV file
file_path = f"{bronze_path}supplier_deliveries_jan2024.csv"

print(f"📖 Reading file: {file_path}")

df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

# Display the data
print(f"\n✓ Successfully loaded {df_raw.count()} records")
print("\n📊 Sample data (first 5 rows):")
df_raw.show(5, truncate=False)

# COMMAND ----------

# Cell 5: Inspect Data Schema
# Verify column names and data types

print("📋 DataFrame Schema:")
df_raw.printSchema()

print("\n📊 Data Types:")
for field in df_raw.schema.fields:
    print(f"  {field.name}: {field.dataType}")

# COMMAND ----------

# Cell 6: Add Audit Columns
# Track when data was ingested and from which file

from pyspark.sql.functions import col

df_bronze = df_raw \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", col("_metadata.file_path")) \
    .withColumn("ingestion_date", lit("2024-01-25"))  # Today's date

print("✓ Added audit columns:")
print("  - ingestion_timestamp: When data entered the data lake")
print("  - source_file: Original file path")
print("  - ingestion_date: Partition key for organization")

print("\n📊 Updated schema:")
df_bronze.printSchema()

print("\n📋 Sample with audit columns:")
df_bronze.select("supplier_id", "product_name", "ingestion_timestamp", "source_file").show(3, truncate=False)

# COMMAND ----------

# Cell 7: Write Bronze Delta Table
# Save as Delta format for ACID transactions and time travel

# Define table name
database_name = "supply_chain"
table_name = "bronze_supplier_deliveries"
full_table_name = f"{database_name}.{table_name}"

# Create database if it doesn't exist
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
print(f"✓ Database '{database_name}' ready")

# Write to Delta table
print(f"\n💾 Writing to Delta table: {full_table_name}")

df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(full_table_name)

print(f"✓ Bronze table created successfully!")

# Verify
row_count = spark.table(full_table_name).count()
print(f"\n📊 Table stats:")
print(f"  - Total records: {row_count}")
print(f"  - Table location: {spark.sql(f'DESCRIBE DETAIL {full_table_name}').select('location').first()[0]}")

# COMMAND ----------

# Cell 8: Query Bronze Table
# Verify data is accessible via SQL

# SQL query
result = spark.sql(f"""
    SELECT 
        supplier_id,
        supplier_name,
        product_name,
        delivery_date,
        quantity,
        delivery_status,
        ingestion_timestamp
    FROM {full_table_name}
    ORDER BY delivery_date
""")

print(f"📊 Bronze Layer Query Results:")
result.show(10, truncate=False)

# Summary statistics
print("\n📈 Delivery Status Distribution:")
spark.sql(f"""
    SELECT 
        delivery_status,
        COUNT(*) as count,
        SUM(quantity) as total_quantity
    FROM {full_table_name}
    GROUP BY delivery_status
    ORDER BY count DESC
""").show()