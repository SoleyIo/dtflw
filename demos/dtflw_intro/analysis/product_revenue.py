# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from dtflw import init_inputs, init_outputs, init_args

args = init_args("year")
inputs = init_inputs("SalesOrderItems")
outputs = init_outputs("ProductRevenueStats")

# COMMAND ----------

sales_order_items_df = (
  spark.read.parquet(inputs["SalesOrderItems"].value)
    .where(f"year(orderDate) == {args['year'].value}")
)

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

product_revenue_stats_df = (
  sales_order_items_df
    .groupBy("productID", "customerID")
    .agg(
      F.sum("revenue").alias("revenue"),
      F.sum("quantity").alias("quantity"),
      F.sum("price").alias("price")
    )
)

product_revenue_stats_df.display()

# COMMAND ----------

product_revenue_stats_df.write.mode("overwrite").parquet(outputs["ProductRevenueStats"].value)