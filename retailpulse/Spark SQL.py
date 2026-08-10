# Databricks notebook source
# MAGIC %md
# MAGIC # Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Filter completed orders 
# MAGIC # 2. Join the three datasets
# MAGIC # 3. Use CAST, CASE WHEN, date functions and string functions.

# COMMAND ----------

completed_orders_joined = spark.sql("""
    SELECT
    o.order_id,
    o.customer_id,
    c.customer_name,
    o.product_id,
    p.product_name,
    date_format(o.order_timestamp, 'yyyy-MM') AS order_month,
    o.quantity,
    CAST(o.quantity * p.unit_price * (1 - o.discount_pct/100) AS DECIMAL(10,2)) AS net_amount,
    c.state,
    c.customer_segment,
    INITCAP(p.category) AS category
FROM retail_fresher.silver_orders o
JOIN retail_fresher.silver_customers c ON o.customer_id = c.customer_id
JOIN retail_fresher.silver_products p ON o.product_id = p.product_id
WHERE o.order_status = 'COMPLETED'
""")

completed_orders_joined.createOrReplaceTempView("completed_orders_joined")

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Create a monthly sales summary.

# COMMAND ----------

monthly_sales = spark.sql("""
    SELECT
    order_month,
    category,
    COUNT(order_id) AS order_count,
    COUNT(quantity) AS total_quantity,
    SUM(net_amount) AS total_revenue,
    ROUND(AVG(net_amount), 2) AS avg_order_value,
    MIN(net_amount) AS min_order_value,
    MAX(net_amount) AS max_order_value
FROM completed_orders_joined
GROUP BY order_month, category
ORDER BY order_month, category
""")
display(monthly_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Rank products by category revenue

# COMMAND ----------

product_revenue_df = spark.sql("""
    SELECT
    category,
    product_id,
    product_name,
    SUM(quantity) AS total_quantity,
    SUM(net_amount) AS total_revenue
    FROM completed_orders_joined
    GROUP BY category, product_id, product_name
""")

product_revenue_df.createOrReplaceTempView("product_revenue")
product_revenue_ranked = spark.sql("""
    SELECT 
    *,
    RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS revenue_rank FROM product_revenue
    ORDER BY category, revenue_rank
""")
display(product_revenue_ranked.filter("revenue_rank <= 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Find the top three customers in each state.

# COMMAND ----------

top_customers_by_state = spark.sql("""
    SELECT
    *,
    DENSE_RANK() OVER (PARTITION BY state ORDER BY lifetime_revenue DESC) AS customer_rank
    FROM retail_fresher.gold_customer_value
    ORDER BY state, customer_rank
""")

display(top_customers_by_state.filter("customer_rank <= 3"))

# COMMAND ----------

# MAGIC %md
# MAGIC # 7. Compare the Spark SQL results with the Pyspark Results

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Unity Catalog catalog name")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = "retail_fresher"

agg_monthly_stats = spark.table(f"{CATALOG}.{SCHEMA}.agg_category_month_stats")

print("Pyspark Monthly Sales Summary: ")
display(agg_monthly_stats)

print("Spark SQL Monthly Sales Summary: ")
display(monthly_sales)