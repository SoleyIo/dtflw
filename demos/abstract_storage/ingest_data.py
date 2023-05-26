# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from dtflw import init_inputs, init_outputs

inputs = init_inputs("Orders", "OrderDetails")
outputs = init_outputs("Orders", "OrderDetails")

# COMMAND ----------

import dataflow

storage = dataflow.get_storage()

# COMMAND ----------

orders_raw_df = (
  spark.read
    .option("inferSchema", False)
    .option("header", True)
    .option("sep", ",")
    .csv(inputs["Orders"].value)
)

order_details_raw_df = (
  spark.read
    .option("inferSchema", False)
    .option("header", True)
    .option("sep", ",")
    .csv(inputs["OrderDetails"].value)
)

# COMMAND ----------

orders_df = (
  orders_raw_df
    .selectExpr(
      "orderID",
      "customerID",
      "to_date(substring(orderDate, 0, 10), 'yyyy-MM-dd') as orderDate",
      "to_date(substring(requiredDate, 0, 10), 'yyyy-MM-dd') as requiredDate",
      "to_date(substring(shippedDate, 0, 10), 'yyyy-MM-dd') as shippedDate",
      "shipCity",
      "shipCountry" 
    )
    .distinct()
)

orders_df.display()

# COMMAND ----------

order_details_df = (
  order_details_raw_df
    .selectExpr(
      "orderID",
      "productID",
      "cast(unitPrice as double) as unitPrice",
      "cast(quantity as double) as quantity",
      "cast(discount as double) as discount"
    )
    .distinct()
)

order_details_df.display()

# COMMAND ----------

storage.write_table(orders_df, outputs["Orders"].value)
storage.write_table(order_details_df, outputs["OrderDetails"].value)