# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from dtflw import init_inputs, init_outputs, init_args

args = init_args("year")
inputs = init_inputs("SalesOrderItems")
outputs = init_outputs("ShippingDelays")

# COMMAND ----------

sales_order_items_df = (
  spark.read.parquet(inputs["SalesOrderItems"].value)
    .where(f"year(orderDate) == {args['year'].value}")
)

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

shipping_delays_df = (
  sales_order_items_df
    .withColumn("delay", F.datediff(F.col("requiredDate"), F.col("shippedDate")))
    .where("delay < 0")
    .groupBy("productID", "customerID", "shipCity", "shipCountry")
    .agg(
      F.sum("revenue").alias("revenue"),
      F.sum("quantity").alias("quantity"),
      F.sum(F.abs("delay")).alias("delay")
    )
    .orderBy(F.desc("delay"))
)

shipping_delays_df.display()

# COMMAND ----------

shipping_delays_df.write.mode("overwrite").parquet(outputs["ShippingDelays"].value)