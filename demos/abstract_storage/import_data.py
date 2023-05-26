# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from dtflw import init_inputs, init_outputs

inputs = init_inputs("Orders", "OrderDetails")
outputs = init_outputs("SalesOrderItems")

# COMMAND ----------

import dataflow

storage = dataflow.get_storage()

# COMMAND ----------

orders_df = storage.read_table(inputs["Orders"].value)
order_details_df = storage.read_table(inputs["OrderDetails"].value)

# COMMAND ----------

import pyspark.sql.functions as F

sales_order_items_df = (
  orders_df.join(order_details_df, "orderId", "inner")
    .withColumn("price", F.col("unitPrice") * F.col("quantity"))
    .withColumn("revenue", F.col("price") - F.col("price") * F.col("discount"))
)

sales_order_items_df.display()

# COMMAND ----------

storage.write_table(sales_order_items_df, outputs["SalesOrderItems"].value)