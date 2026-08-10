# Databricks notebook source
# MAGIC %md
# MAGIC # Part C — Aggregation and Window Functions
# MAGIC Demonstrates each required aggregation/window pattern explicitly against the
# MAGIC Bronze/Silver/Gold Delta tables built in the previous two notebooks.
# MAGIC
# MAGIC Run `01_bronze_ingestion` and `02_silver_gold_transformations` first.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog catalog name")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = "retail_fresher"

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver_sales = spark.table(f"{CATALOG}.{SCHEMA}.silver_sales_enriched")
silver_customers = spark.table(f"{CATALOG}.{SCHEMA}.silver_customers")
bronze_customers = spark.table(f"{CATALOG}.{SCHEMA}.bronze_customers")
gold_customer_value = spark.table(f"{CATALOG}.{SCHEMA}.gold_customer_value")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. GROUP BY category and month — SUM, COUNT, AVG, MIN, MAX

# COMMAND ----------

category_month_stats_df = (
    silver_sales
    .groupBy("order_month", "category")
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("net_sales").alias("total_revenue"),
        F.round(F.avg("net_sales"), 2).alias("avg_order_value"),
        F.min("net_sales").alias("min_order_value"),
        F.max("net_sales").alias("max_order_value"),
    )
    .orderBy("order_month", "category")
)

display(category_month_stats_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. row_number() — latest customer profile
# MAGIC Same technique used to build `silver_customers`, shown here standalone
# MAGIC against the raw Bronze table for demonstration.

# COMMAND ----------

latest_profile_window = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())

latest_customer_profile_df = (
    bronze_customers
    .withColumn("updated_at_ts", F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss"))
    .withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("customer_id").orderBy(F.col("updated_at_ts").desc())
        ),
    )
    .filter(F.col("rn") == 1)
    .drop("rn", "updated_at_ts")
)

print("bronze_customers rows        :", bronze_customers.count())
print("deduped latest-profile rows  :", latest_customer_profile_df.count())
display(latest_customer_profile_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. rank() — top products inside each category

# COMMAND ----------

product_revenue_df = (
    silver_sales
    .groupBy("category", "product_id", "product_name")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("net_sales").alias("total_revenue"),
    )
)

category_window = Window.partitionBy("category").orderBy(F.col("total_revenue").desc())

top_products_ranked_df = (
    product_revenue_df
    .withColumn("revenue_rank", F.rank().over(category_window))
    .orderBy("category", "revenue_rank")
)

display(top_products_ranked_df.filter(F.col("revenue_rank") <= 5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. dense_rank() — customers ranked within each state
# MAGIC Ranked by lifetime revenue (`gold_customer_value`). `dense_rank()` is used
# MAGIC instead of `rank()` so tied revenue values don't skip rank numbers.

# COMMAND ----------

state_window = Window.partitionBy("state").orderBy(F.col("lifetime_revenue").desc())

customers_ranked_by_state_df = (
    gold_customer_value
    .withColumn("state_rank", F.dense_rank().over(state_window))
    .orderBy("state", "state_rank")
)

display(customers_ranked_by_state_df.filter(F.col("state_rank") <= 3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Running revenue total by category and month
# MAGIC Cumulative sum of monthly revenue within each category, ordered chronologically.

# COMMAND ----------

category_month_revenue_df = (
    silver_sales
    .groupBy("category", "order_month")
    .agg(F.sum("net_sales").alias("monthly_revenue"))
)

running_total_window = (
    Window.partitionBy("category")
    .orderBy("order_month")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

running_revenue_df = (
    category_month_revenue_df
    .withColumn("running_revenue_total", F.sum("monthly_revenue").over(running_total_window))
    .orderBy("category", "order_month")
)

display(running_revenue_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Latest order for every customer

# COMMAND ----------

latest_order_window = Window.partitionBy("customer_id").orderBy(F.col("order_timestamp").desc())

latest_order_per_customer_df = (
    silver_sales
    .withColumn("rn", F.row_number().over(latest_order_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .select(
        "customer_id", "customer_name", "order_id", "order_timestamp",
        "product_name", "category", "net_sales", "order_status",
    )
    .orderBy("customer_id")
)

display(latest_order_per_customer_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Persist Part C results as Delta tables (optional, for reuse in Part D)

# COMMAND ----------

category_month_stats_df.write.mode("overwrite").format("delta").saveAsTable(
    f"{CATALOG}.{SCHEMA}.agg_category_month_stats"
)
running_revenue_df.write.mode("overwrite").format("delta").saveAsTable(
    f"{CATALOG}.{SCHEMA}.agg_running_revenue_by_category"
)
latest_order_per_customer_df.write.mode("overwrite").format("delta").saveAsTable(
    f"{CATALOG}.{SCHEMA}.agg_latest_order_per_customer"
)

print("Part C complete — aggregation/window tables written under", f"{CATALOG}.{SCHEMA}")