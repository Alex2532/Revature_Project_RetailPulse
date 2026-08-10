# Databricks notebook source
# MAGIC %md
# MAGIC # Part B — PySpark Transformations (Silver + Gold)
# MAGIC Cleans the three Bronze tables, casts real types, deduplicates and filters,
# MAGIC derives the sales metrics, joins everything into an enriched Silver table,
# MAGIC then builds four Gold analytical tables.
# MAGIC
# MAGIC Run `01_bronze_ingestion` first.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog catalog name")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = "retail_fresher"

from pyspark.sql import functions as F
from pyspark.sql.window import Window

bronze_customers = spark.table(f"{CATALOG}.{SCHEMA}.bronze_customers")
bronze_products = spark.table(f"{CATALOG}.{SCHEMA}.bronze_products")
bronze_orders = spark.table(f"{CATALOG}.{SCHEMA}.bronze_sales_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display schema and sample rows

# COMMAND ----------

bronze_customers.printSchema()
display(bronze_customers.limit(5))

# COMMAND ----------

bronze_products.printSchema()
display(bronze_products.limit(5))

# COMMAND ----------

bronze_orders.printSchema()
display(bronze_orders.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Clean & cast — Customers
# MAGIC - Trim every string column
# MAGIC - Title-case `customer_name`/`city`, upper-case `state`/`region`/`customer_segment`
# MAGIC - Replace missing `city` with `Unknown`
# MAGIC - Cast `signup_date`, `date_of_birth` to date; `updated_at` to timestamp; `loyalty_points` to int
# MAGIC - Deduplicate on `customer_id`, keeping the row with the latest `updated_at` (row_number)
# MAGIC - Filter out inactive customers (`is_active` != 'Y')

# COMMAND ----------

customers_trimmed = bronze_customers
for c in ["customer_id", "customer_name", "email", "city", "state", "region",
          "customer_segment", "is_active"]:
    customers_trimmed = customers_trimmed.withColumn(c, F.trim(F.col(c)))

customers_typed = (
    customers_trimmed
    .withColumn("customer_name", F.initcap(F.col("customer_name")))
    .withColumn("city", F.when((F.col("city") == "") | F.col("city").isNull(), "Unknown")
                          .otherwise(F.initcap(F.col("city"))))
    .withColumn("state", F.initcap(F.col("state")))
    .withColumn("region", F.upper(F.col("region")))
    .withColumn("customer_segment", F.initcap(F.col("customer_segment")))
    .withColumn("is_active", F.upper(F.col("is_active")))
    .withColumn("signup_date", F.to_date("signup_date", "yyyy-MM-dd"))
    .withColumn("date_of_birth", F.to_date("date_of_birth", "yyyy-MM-dd"))
    .withColumn("updated_at", F.to_timestamp("updated_at", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("loyalty_points", F.col("loyalty_points").cast("int"))
)

# Deduplicate: keep the latest updated_at per customer_id
dedup_window = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())

customers_deduped = (
    customers_typed
    .withColumn("rn", F.row_number().over(dedup_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

silver_customers_df = customers_deduped.filter(F.col("is_active") == "Y")

print("bronze customers        :", bronze_customers.count())
print("after dedup             :", customers_deduped.count())
print("silver (active only)    :", silver_customers_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Clean & cast — Products
# MAGIC - Trim strings, title-case `product_name`/`subcategory`, upper-case `category`
# MAGIC - Cast `unit_price`/`cost_price` to decimal, `stock_quantity` to int, `launch_date` to date, `product_rating` to double
# MAGIC - Remove rows with invalid (null, non-numeric, or <= 0) price/cost
# MAGIC - Filter out inactive products (`active_flag` != 'Y')

# COMMAND ----------

products_trimmed = bronze_products
for c in ["product_id", "product_name", "category", "subcategory",
          "supplier_name", "active_flag"]:
    products_trimmed = products_trimmed.withColumn(c, F.trim(F.col(c)))

products_typed = (
    products_trimmed
    .withColumn("product_name", F.initcap(F.col("product_name")))
    .withColumn("category", F.initcap(F.col("category")))
    .withColumn("subcategory", F.initcap(F.col("subcategory")))
    .withColumn("supplier_name", F.initcap(F.col("supplier_name")))
    .withColumn("active_flag", F.upper(F.col("active_flag")))
    .withColumn("unit_price", F.expr("try_cast(unit_price as decimal(12,2))"))
    .withColumn("cost_price", F.expr("try_cast(cost_price as decimal(12,2))"))
    .withColumn("stock_quantity", F.col("stock_quantity").cast("int"))
    .withColumn("launch_date", F.to_date("launch_date", "yyyy-MM-dd"))
    .withColumn("product_rating", F.col("product_rating").cast("double"))
)

products_valid_price = products_typed.filter(
    F.col("unit_price").isNotNull() & (F.col("unit_price") > 0) &
    F.col("cost_price").isNotNull() & (F.col("cost_price") > 0)
)

silver_products_df = products_valid_price.filter(F.col("active_flag") == "Y")

print("bronze products          :", bronze_products.count())
print("after valid price/cost   :", products_valid_price.count())
print("silver (active only)     :", silver_products_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Clean & cast — Orders
# MAGIC - Trim & upper-case status/channel/payment columns
# MAGIC - Cast timestamps, dates, `quantity` to int, `discount_pct` to decimal
# MAGIC - Filter out `quantity <= 0`
# MAGIC - Exclude `PENDING` and `CANCELLED` from the financial (Silver/Gold) view —
# MAGIC   the full cleaned order set is still kept separately for reference

# COMMAND ----------

orders_trimmed = bronze_orders
for c in ["order_id", "customer_id", "product_id", "payment_method",
          "order_status", "sales_channel", "warehouse_id"]:
    orders_trimmed = orders_trimmed.withColumn(c, F.trim(F.col(c)))

orders_typed = (
    orders_trimmed
    .withColumn("order_timestamp", F.to_timestamp("order_timestamp", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("promised_delivery_date", F.to_date("promised_delivery_date", "yyyy-MM-dd"))
    .withColumn("actual_delivery_date", F.to_date("actual_delivery_date", "yyyy-MM-dd"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("discount_pct", F.col("discount_pct").cast("decimal(5,2)"))
    .withColumn("payment_method", F.upper(F.col("payment_method")))
    .withColumn("order_status", F.upper(F.col("order_status")))
    .withColumn("sales_channel", F.upper(F.col("sales_channel")))
    .withColumn("warehouse_id", F.upper(F.col("warehouse_id")))
)

orders_qty_valid = orders_typed.filter(F.col("quantity") > 0)

# Full cleaned order set (all statuses, qty-valid) — kept for reference/QA
silver_orders_all_df = orders_qty_valid

# Financial subset: drop PENDING and CANCELLED
orders_financial = orders_qty_valid.filter(
    ~F.col("order_status").isin("PENDING", "CANCELLED")
)

print("bronze orders            :", bronze_orders.count())
print("after qty > 0 filter     :", orders_qty_valid.count())
print("financial subset         :", orders_financial.count())
display(orders_typed.groupBy("order_status").count().orderBy("order_status"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Derived columns
# MAGIC `gross_amount`, `discount_amount`, `net_amount`, `net_sales`, `profit_per_unit`,
# MAGIC `delivery_days`, `late_delivery_flag`, `order_month`.
# MAGIC
# MAGIC These need product price/cost, so they're computed after joining orders to
# MAGIC (Silver) products — see the join step below, which produces the enriched
# MAGIC Silver sales table directly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Join customers, products and orders → enriched Silver sales table

# COMMAND ----------

silver_sales_enriched_df = (
    orders_financial.alias("o")
    .join(silver_products_df.alias("p"), on="product_id", how="inner")
    .join(silver_customers_df.alias("c"), on="customer_id", how="inner")
    .withColumn("gross_amount", (F.col("o.quantity") * F.col("p.unit_price")).cast("decimal(14,2)"))
    .withColumn("discount_amount",
                (F.col("gross_amount") * (F.col("o.discount_pct") / F.lit(100))).cast("decimal(14,2)"))
    .withColumn("net_amount", (F.col("gross_amount") - F.col("discount_amount")).cast("decimal(14,2)"))
    .withColumn("net_sales", F.col("net_amount"))  # net_sales mirrors net_amount for aggregation convenience
    .withColumn("profit_per_unit", (F.col("p.unit_price") - F.col("p.cost_price")).cast("decimal(12,2)"))
    .withColumn("delivery_days", F.datediff(F.col("o.actual_delivery_date"), F.col("o.promised_delivery_date")))
    .withColumn("late_delivery_flag",
                F.when(F.col("o.actual_delivery_date") > F.col("o.promised_delivery_date"), F.lit("Y"))
                 .otherwise(F.lit("N")))
    .withColumn("order_month", F.date_format(F.col("o.order_timestamp"), "yyyy-MM"))
    .select(
        "o.order_id", "o.order_timestamp", "order_month",
        "o.customer_id", "c.customer_name", "c.city", "c.state", "c.region", "c.customer_segment",
        "o.product_id", "p.product_name", "p.category", "p.subcategory",
        "o.quantity", "p.unit_price", "p.cost_price", "o.discount_pct",
        "gross_amount", "discount_amount", "net_amount", "net_sales", "profit_per_unit",
        "o.promised_delivery_date", "o.actual_delivery_date", "delivery_days", "late_delivery_flag",
        "o.order_status", "o.payment_method", "o.sales_channel", "o.warehouse_id",
    )
)

display(silver_sales_enriched_df.limit(10))
print("silver_sales_enriched rows:", silver_sales_enriched_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write Silver managed Delta tables

# COMMAND ----------

silver_customers_df.write.mode("overwrite").format("delta").saveAsTable(
    f"{CATALOG}.{SCHEMA}.silver_customers"
)
silver_products_df.write.mode("overwrite").format("delta").saveAsTable(
    f"{CATALOG}.{SCHEMA}.silver_products"
)
silver_orders_all_df.write.mode("overwrite").format("delta").saveAsTable(
    f"{CATALOG}.{SCHEMA}.silver_orders"
)
silver_sales_enriched_df.write.mode("overwrite").format("delta").saveAsTable(
    f"{CATALOG}.{SCHEMA}.silver_sales_enriched"
)

for t in ["silver_customers", "silver_products", "silver_orders", "silver_sales_enriched"]:
    n = spark.table(f"{CATALOG}.{SCHEMA}.{t}").count()
    print(f"  {t}: {n} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Gold tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7a. Monthly category sales

# COMMAND ----------

gold_monthly_category_sales_df = (
    silver_sales_enriched_df
    .groupBy("order_month", "category")
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("net_sales").alias("total_revenue"),
        F.round(F.avg("net_sales"), 2).alias("avg_order_value"),
    )
    .orderBy("order_month", "category")
)

display(gold_monthly_category_sales_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7b. City sales

# COMMAND ----------

gold_city_sales_df = (
    silver_sales_enriched_df
    .groupBy("city", "state", "region")
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("net_sales").alias("total_revenue"),
    )
    .orderBy(F.col("total_revenue").desc())
)

display(gold_city_sales_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7c. Customer value

# COMMAND ----------

gold_customer_value_df = (
    silver_sales_enriched_df
    .groupBy("customer_id", "customer_name", "customer_segment", "state")
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum("net_sales").alias("lifetime_revenue"),
        F.round(F.avg("net_sales"), 2).alias("avg_order_value"),
        F.max("order_timestamp").alias("last_order_at"),
    )
    .orderBy(F.col("lifetime_revenue").desc())
)

display(gold_customer_value_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7d. Top products by category (by revenue, using rank())

# COMMAND ----------

product_sales = (
    silver_sales_enriched_df
    .groupBy("category", "product_id", "product_name")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("net_sales").alias("total_revenue"),
    )
)

category_rank_window = Window.partitionBy("category").orderBy(F.col("total_revenue").desc())

gold_top_products_by_category_df = (
    product_sales
    .withColumn("revenue_rank", F.rank().over(category_rank_window))
    .filter(F.col("revenue_rank") <= 5)
    .orderBy("category", "revenue_rank")
)

display(gold_top_products_by_category_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write Gold managed Delta tables

# COMMAND ----------

gold_monthly_category_sales_df.write.mode("overwrite").format("delta").saveAsTable(
    f"{CATALOG}.{SCHEMA}.gold_monthly_category_sales"
)
gold_city_sales_df.write.mode("overwrite").format("delta").saveAsTable(
    f"{CATALOG}.{SCHEMA}.gold_city_sales"
)
gold_customer_value_df.write.mode("overwrite").format("delta").saveAsTable(
    f"{CATALOG}.{SCHEMA}.gold_customer_value"
)
gold_top_products_by_category_df.write.mode("overwrite").format("delta").saveAsTable(
    f"{CATALOG}.{SCHEMA}.gold_top_products_by_category"
)

for t in ["gold_monthly_category_sales", "gold_city_sales",
          "gold_customer_value", "gold_top_products_by_category"]:
    n = spark.table(f"{CATALOG}.{SCHEMA}.{t}").count()
    print(f"  {t}: {n} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Quick validation summary

# COMMAND ----------

validation_rows = [
    ("customers", bronze_customers.count(), customers_deduped.count(), silver_customers_df.count()),
    ("products", bronze_products.count(), products_valid_price.count(), silver_products_df.count()),
    ("orders", bronze_orders.count(), orders_qty_valid.count(), orders_financial.count()),
]
validation_df = spark.createDataFrame(
    validation_rows, ["dataset", "bronze_rows", "after_dedup_or_validity_filter", "silver_rows"]
)
display(validation_df)

print("Silver/Gold layer complete. Continue to 03_aggregations_and_windows for Part C.")