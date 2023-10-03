# Abstract storage in notebooks
This project demonstrates how to remove technical details of tables reading and writing from notebooks.

## Requirements
1. Shared Spark session between notebooks. To enable it, add to Spark config: `spark.databricks.session.share true`.
2. Run the project from Repos (so that callee notebook can import dataflow).
3. dataflow.py must be a python file in the root of a repo.

## Get Started
1. Import data files from `demos/data/` to `dbfs:/FileStore/tables/dtflw_data/` directory.
2. Import notebooks to a repo.

## References
- `demo/data/` used in this demo was taken from [here](https://github.com/graphql-compose/graphql-compose-examples/tree/master/examples/northwind/data/csv). 
