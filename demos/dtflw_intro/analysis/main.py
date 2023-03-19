# Databricks notebook source
# This is an example of cascaded notebooks.

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

from dtflw import init_inputs, init_outputs, init_args

args = init_args("year")
inputs = init_inputs("SalesOrderItems")
outputs = init_outputs("ProductRevenueStats", "ShippingDelays")

# COMMAND ----------

from dtflw import init_flow
from dtflw.storage.dbfs import init_storage

storage = init_storage()
flow = init_flow(storage)
is_lazy = True

# COMMAND ----------

(
  flow.notebook("product_revenue")
    .args({
      "year": "1997"
    })
    .input("SalesOrderItems", file_path=inputs["SalesOrderItems"].value)
    .output("ProductRevenueStats", file_path=outputs["ProductRevenueStats"].value)
    .run(is_lazy)
)

# COMMAND ----------

(
  flow.notebook("shipping_delays")
    .args({
      "year": "1997"
    })
    .input("SalesOrderItems", file_path=inputs["SalesOrderItems"].value)
    .output("ShippingDelays", file_path=outputs["ShippingDelays"].value)
    .run(is_lazy)
)