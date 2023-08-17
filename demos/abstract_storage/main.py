# Databricks notebook source
# Requirements
# ------------
# 1. Add to Spark config: 'spark.databricks.session.share true'.
# 2. Run it from Repos (so that callee notebook can import dataflow).
# 3. dataflow.py must be a python file in the root of a repo.

# COMMAND ----------

import dataflow

flow = dataflow.get_flow()
# storage instance is also available at flow.ctx.storage
storage = dataflow.get_storage()

# COMMAND ----------

flow.ctx.logger.verbosity = "verbose"

# COMMAND ----------

(
  flow.notebook("ingest_data")
    .input("Orders", file_path = storage.get_abs_path("FileStore/tables/dtflw_data/orders.csv"))
    .input("OrderDetails", storage.get_abs_path("FileStore/tables/dtflw_data/order_details.csv"))
    .output("Orders", cols=[
      ('orderID', 'string'),
      ('customerID', 'string'),
      ('orderDate', 'date'),
      ('requiredDate', 'date'),
      ('shippedDate', 'date'),
      ('shipCity', 'string'),
      ('shipCountry', 'string')
    ])
    .output("OrderDetails", cols=[
      ('orderID', 'string'),
      ('productID', 'string'),
      ('unitPrice', 'double'),
      ('quantity', 'double'),
      ('discount', 'double')
    ])
    .run(is_lazy, strict_validation=True)
)

# COMMAND ----------

(
  flow.notebook("import_data")
    .input("Orders")
    .input("OrderDetails")
    .output(
      "SalesOrderItems", 
      cols=[
        ('price', 'double'),
        ('revenue', 'double')
      ],
      alias="SalesOrderPositions"
    )
    .run(is_lazy, strict_validation=False)
)

# COMMAND ----------

(
  flow.notebook("analysis/main")
    .args({
      "year": "1997"
    })
    .input("SalesOrderItems", source_table="SalesOrderPositions")
    .output("ProductRevenueStats")
    .output("ShippingDelays")
    .run(is_lazy)
)

# COMMAND ----------

flow.show()