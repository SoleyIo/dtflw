# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from dtflw import init_args, init_inputs, init_outputs

args = init_args("year")
inputs = init_inputs("SalesOrderItems")
outputs = init_outputs("ProductRevenueStats", "ShippingDelays")

is_lazy = True

# COMMAND ----------

import dataflow

flow = dataflow.get_flow()
storage = dataflow.get_storage()

# COMMAND ----------

(
  flow.notebook("product_revenue")
    .args({
      "year": args["year"].value
    })
    .input("SalesOrderItems", file_path=inputs["SalesOrderItems"].value)
    .output("ProductRevenueStats", file_path=outputs["ProductRevenueStats"].value)
    .run(is_lazy)
)

# COMMAND ----------

(
  flow.notebook("shipping_delays")
    .args({
      "year": args["year"].value
    })
    .input("SalesOrderItems", file_path=inputs["SalesOrderItems"].value)
    .output("ShippingDelays", file_path=outputs["ShippingDelays"].value)
    .run(is_lazy)
)
