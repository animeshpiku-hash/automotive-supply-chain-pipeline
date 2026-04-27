# Databricks notebook source
# Cell 1: Configuration (Silver Layer)

# Same as Bronze (use placeholder for GitHub safety)
storage_account_name = "adlssupplychainam"
storage_account_key = "YOUR_ACCESS_KEY"

silver_container = "silver"
bronze_container = "bronze"

# Set Spark config
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# Define paths
bronze_path = f"abfss://{bronze_container}@{storage_account_name}.dfs.core.windows.net/"
silver_path = f"abfss://{silver_container}@{storage_account_name}.dfs.core.windows.net/"

print("✓ Silver layer configuration ready")
print(f"Bronze path: {bronze_path}")
print(f"Silver path: {silver_path}")

# COMMAND ----------

# Cell 2: Read Bronze Layer Data
# Silver layer takes bronze as input

from pyspark.sql.functions import *

# Read from bronze Delta table
bronze_table = "supply_chain.bronze_supplier_deliveries"

print(f"📖 Reading from: {bronze_table}")

df_bronze = spark.table(bronze_table)

print(f"✓ Loaded {df_bronze.count()} records from bronze")
print("\n📊 Bronze data sample:")
df_bronze.show(5)

# COMMAND ----------

# Cell 3: Data Quality Assessment
# Identify issues before cleaning

print("🔍 DATA QUALITY ASSESSMENT")
print("=" * 50)

# 1. Check for nulls in critical columns
critical_columns = ["supplier_id", "product_id", "delivery_date", "quantity"]

print("\n1️⃣ NULL VALUE CHECK:")

total = df_bronze.count()   # calculate once (optimization)

for column_name in critical_columns:
    null_count = df_bronze.filter(col(column_name).isNull()).count()
    print(f"   {column_name}: {null_count}/{total} nulls ({null_count/total*100:.1f}%)")

# 2. Check for duplicates
print("\n2️⃣ DUPLICATE CHECK:")
duplicate_count = df_bronze.count() - df_bronze.dropDuplicates(["supplier_id", "product_id", "delivery_date"]).count()
print(f"   Duplicate records: {duplicate_count}")

# 3. Check value ranges
print("\n3️⃣ VALUE RANGE CHECK:")
print("   Quantity statistics:")
df_bronze.select(
    min("quantity").alias("min"),
    max("quantity").alias("max"),
    avg("quantity").alias("avg")
).show()

# 4. Check delivery status values
print("\n4️⃣ DELIVERY STATUS VALUES:")
df_bronze.groupBy("delivery_status").count().show()

# 5. Check for future dates (data quality issue)
print("\n5️⃣ DATE VALIDATION:")
from datetime import datetime
future_dates = df_bronze.filter(col("delivery_date") > lit(datetime.now().strftime("%Y-%m-%d"))).count()
print(f"   Future dates found: {future_dates}")

# COMMAND ----------

# Cell 4: Data Cleansing and Standardization
# Transform bronze → silver

print("🧹 CLEANING AND STANDARDIZING DATA")
print("=" * 50)

# Start with bronze data
df_silver = df_bronze

# STEP 1: Remove duplicates
print("\n1️⃣ Removing duplicates...")
initial_count = df_silver.count()
df_silver = df_silver.dropDuplicates(["supplier_id", "product_id", "delivery_date"])
removed = initial_count - df_silver.count()
print(f"   Removed {removed} duplicate records")

# STEP 2: Handle null values
print("\n2️⃣ Handling null values...")
# Drop rows where critical columns are null
df_silver = df_silver.filter(
    col("supplier_id").isNotNull() &
    col("product_id").isNotNull() &
    col("delivery_date").isNotNull() &
    col("quantity").isNotNull()
)
print(f"   Rows after null removal: {df_silver.count()}")

# STEP 3: Standardize email addresses (lowercase, trim spaces)
print("\n3️⃣ Standardizing contact emails...")
df_silver = df_silver.withColumn(
    "contact_email",
    lower(trim(col("contact_email")))
)

# STEP 4: Standardize country names (uppercase)
print("\n4️⃣ Standardizing country names...")
df_silver = df_silver.withColumn(
    "country",
    upper(trim(col("country")))
)

# STEP 5: Standardize delivery status (Title Case)
print("\n5️⃣ Standardizing delivery status...")
df_silver = df_silver.withColumn(
    "delivery_status",
    initcap(col("delivery_status"))  # First letter uppercase
)

# STEP 6: Data type conversions (ensure proper types)
print("\n6️⃣ Converting data types...")
df_silver = df_silver.withColumn(
    "delivery_date",
    to_date(col("delivery_date"))
)
df_silver = df_silver.withColumn(
    "quantity",
    col("quantity").cast("integer")
)

# STEP 7: Add data quality flag
print("\n7️⃣ Adding quality validation flag...")
df_silver = df_silver.withColumn(
    "is_valid_quantity",
    when(col("quantity") > 0, True).otherwise(False)
)

print("\n✓ Cleaning complete!")
print(f"   Final record count: {df_silver.count()}")

# COMMAND ----------

# Cell 5: Add Silver Layer Metadata
# Track transformation timestamp

from pyspark.sql.functions import current_timestamp, lit

df_silver = df_silver \
    .withColumn("silver_processed_timestamp", current_timestamp()) \
    .withColumn("data_quality_score", lit(100))  # We'll calculate this later

print("✓ Added silver layer metadata columns")
print("\n📋 Final Silver Schema:")
df_silver.printSchema()

print("\n📊 Sample transformed data:")
df_silver.select(
    "supplier_id",
    "supplier_name", 
    "contact_email",
    "country",
    "delivery_status",
    "is_valid_quantity",
    "silver_processed_timestamp"
).show(5, truncate=False)

# COMMAND ----------

# Cell 6: Write to Silver Delta Table
# Save cleaned data to silver layer

silver_table = "supply_chain.silver_supplier_deliveries"

print(f"💾 Writing to silver Delta table: {silver_table}")

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(silver_table)

print(f"✓ Silver table created successfully!")

# Verify
row_count = spark.table(silver_table).count()
print(f"\n📊 Silver Layer Stats:")
print(f"   Total records: {row_count}")

# Show data quality summary
print("\n📈 Data Quality Summary:")
spark.sql(f"""
    SELECT 
        delivery_status,
        COUNT(*) as record_count,
        SUM(quantity) as total_quantity,
        AVG(CASE WHEN is_valid_quantity THEN 1 ELSE 0 END) * 100 as quality_percent
    FROM {silver_table}
    GROUP BY delivery_status
    ORDER BY record_count DESC
""").show()

# COMMAND ----------

# Cell 7: Bronze vs Silver Comparison
# Show the transformation impact

print("📊 BRONZE vs SILVER COMPARISON")
print("=" * 60)

bronze_count = spark.table("supply_chain.bronze_supplier_deliveries").count()
silver_count = spark.table("supply_chain.silver_supplier_deliveries").count()

print(f"\n📈 Record Counts:")
print(f"   Bronze: {bronze_count} records")
print(f"   Silver: {silver_count} records")
print(f"   Difference: {bronze_count - silver_count} records removed (duplicates/invalid)")

print(f"\n✅ Data Quality Improvement:")
print(f"   - Duplicates removed")
print(f"   - Null values handled")
print(f"   - Emails standardized (lowercase)")
print(f"   - Countries standardized (UPPERCASE)")
print(f"   - Delivery status standardized (Title Case)")
print(f"   - Data types validated")

# Show side-by-side comparison
print("\n🔍 Sample Transformation (Bronze → Silver):")
print("\nBRONZE (before):")
spark.table("supply_chain.bronze_supplier_deliveries") \
    .select("contact_email", "country", "delivery_status") \
    .show(3, truncate=False)

print("SILVER (after):")
spark.table("supply_chain.silver_supplier_deliveries") \
    .select("contact_email", "country", "delivery_status") \
    .show(3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC