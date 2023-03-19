# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from dtflw import init_inputs, init_outputs

inputs = init_inputs("Orders", "OrderDetails")
outputs = init_outputs("SalesOrderItems")

# COMMAND ----------

orders_df = spark.read.parquet(inputs["Orders"].value)
order_details_df = spark.read.parquet(inputs["OrderDetails"].value)

# COMMAND ----------

import pyspark.sql.functions as F

sales_order_items_df = (
  orders_df.join(order_details_df, "orderId", "inner")
    .withColumn("price", F.col("unitPrice") * F.col("quantity"))
    .withColumn("revenue", F.col("price") - F.col("price") * F.col("discount"))
)

sales_order_items_df.display()

# COMMAND ----------

sales_order_items_df.write.mode("overwrite").parquet(outputs["SalesOrderItems"].value)