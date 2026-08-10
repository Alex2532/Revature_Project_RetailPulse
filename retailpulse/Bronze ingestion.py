# Databricks notebook source
# MAGIC %md
# MAGIC # Part A — Databricks Setup and Bronze Layer
# MAGIC RetailPulse Weekly Sales Intelligence — Bronze ingestion notebook.
# MAGIC
# MAGIC Steps:
# MAGIC 1. Create schema `retail_fresher`
# MAGIC 2. Create managed volume `retail_raw` inside it
# MAGIC 3. Upload the three CSVs into the volume (via UI or `dbutils.fs.cp`)
# MAGIC 4. Read each CSV with PySpark, keep all columns as strings
# MAGIC 5. Add `source_file` and `ingestion_timestamp`
# MAGIC 6. Write managed Delta tables: bronze_customers, bronze_products, bronze_sales_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Config
# MAGIC Set your catalog name. In Databricks Free Edition this is usually `workspace`
# MAGIC unless your workspace admin created a different Unity Catalog catalog for you.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog catalog name")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = "retail_fresher"
VOLUME = "retail_raw"

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

print(f"Catalog : {CATALOG}")
print(f"Schema  : {SCHEMA}")
print(f"Volume  : {VOLUME}")
print(f"Path    : {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create schema and managed volume

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Upload the CSVs into the volume
# MAGIC Easiest path: open **Catalog Explorer → workspace → retail_fresher → retail_raw**
# MAGIC and use **Upload to this volume** to drop in:
# MAGIC - `customers_500.csv`
# MAGIC - `products_500.csv`
# MAGIC - `sales_orders_500.csv`
# MAGIC
# MAGIC If instead you already have them staged somewhere reachable by the driver
# MAGIC (e.g. `/tmp` after a `%sh curl`/DBFS upload), you can copy them in with
# MAGIC `dbutils.fs.cp` — uncomment and edit the three lines below.

# COMMAND ----------

# dbutils.fs.cp("file:/tmp/customers_500.csv",    f"{VOLUME_PATH}/customers_500.csv")
# dbutils.fs.cp("file:/tmp/products_500.csv",     f"{VOLUME_PATH}/products_500.csv")
# dbutils.fs.cp("file:/tmp/sales_orders_500.csv", f"{VOLUME_PATH}/sales_orders_500.csv")

display(dbutils.fs.ls(VOLUME_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read every CSV with PySpark
# MAGIC All Bronze columns are kept as **strings** — no casting yet. Casting to real
# MAGIC types happens later in Silver (Part B), which is standard medallion practice:
# MAGIC Bronze preserves the source data as-is for auditability.

# COMMAND ----------

from pyspark.sql import functions as F

def read_csv_as_strings(path):
    """Read a CSV, inferring only the column names/order, then cast every
    column to string so Bronze faithfully mirrors the raw source file."""
    raw = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(path)
    )
    return raw.select([F.col(c).cast("string") for c in raw.columns])

customers_raw = read_csv_as_strings(f"{VOLUME_PATH}/customers_500.csv")
products_raw = read_csv_as_strings(f"{VOLUME_PATH}/products_500.csv")
orders_raw = read_csv_as_strings(f"{VOLUME_PATH}/sales_orders_500.csv")

print("customers_raw:", customers_raw.count(), "rows")
print("products_raw :", products_raw.count(), "rows")
print("orders_raw   :", orders_raw.count(), "rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Add `source_file` and `ingestion_timestamp`

# COMMAND ----------

def add_bronze_metadata(df, source_file_name):
    return (
        df.withColumn("source_file", F.lit(source_file_name))
          .withColumn("ingestion_timestamp", F.current_timestamp())
    )

bronze_customers_df = add_bronze_metadata(customers_raw, "customers_500.csv")
bronze_products_df = add_bronze_metadata(products_raw, "products_500.csv")
bronze_sales_orders_df = add_bronze_metadata(orders_raw, "sales_orders_500.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write managed Delta tables

# COMMAND ----------

bronze_customers_df.write.mode("overwrite").format("delta").saveAsTable(
    f"{CATALOG}.{SCHEMA}.bronze_customers"
)
bronze_products_df.write.mode("overwrite").format("delta").saveAsTable(
    f"{CATALOG}.{SCHEMA}.bronze_products"
)
bronze_sales_orders_df.write.mode("overwrite").format("delta").saveAsTable(
    f"{CATALOG}.{SCHEMA}.bronze_sales_orders"
)

print("Bronze tables written:")
for t in ["bronze_customers", "bronze_products", "bronze_sales_orders"]:
    n = spark.table(f"{CATALOG}.{SCHEMA}.{t}").count()
    print(f"  {t}: {n} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Sanity check

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA}.bronze_customers").limit(5))

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA}.bronze_products").limit(5))

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA}.bronze_sales_orders").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Bronze layer complete: `bronze_customers`, `bronze_products`, `bronze_sales_orders`
# MAGIC are now managed Delta tables in `retail_fresher`, all-string schema, each
# MAGIC tagged with its source file and load time. Continue to **02_silver_gold_transformations**.