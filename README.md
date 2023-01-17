# dtflw / dataflow

`dtflw` is a Python framework for building modular data pipelines based on [Databricks dbutils.notebook API](https://docs.databricks.com/notebooks/notebook-workflows.html). It was conceived with an intention to facilitate development and maintenance of Databricks data pipelines.

## Why `dtflw`
[Databricks](https://docs.databricks.com/notebooks/index.html) offers everything necessary to organize and manage code of data pipelines according to different requirements and tastes. Also, it does not impose any specific file structure on a repo of a pipeline neither regulates relationships between data (tables) and code transforming them (notebooks).

In general, such freedom is an advantage, but with a growing number of notebooks, variety of data and complexity of analysis logic
>_it gets laborious to work with a codebase of a pipeline while debugging, extending or refactoring it._

Among dozens of notebooks of a pipeline and thousands lines of code, `it is difficult to keep in mind which table a notebooks requires to work and what tables it produces`. On the other side, when exploring tables (files) on a storage (e.g. Azure Blob, AWS S3), `it is unclear which code wrote those tables`.

The complexity rises even more when a team needs to `maintain many of such pipelines which are organized without a uniform structural pattern`.

## How `dtflw` works
This project identifies `implicit relationships between data and code in a pipeline` as the main reason for increasing complexity.

Therefore, `dtflw` makes relationships between tables and notebooks explicit by building around a simple dataflow model:
> Each notebook of a pipeline
> 1. consumes input tables (possibly none), 
> 2. produces output tables (possibly none),
> 3. and may require additional arguments to run.  
>
> Thus, a pipeline is a sequence of notebooks chained by input-output tables.

Here is an example of a Databricks pipeline built using `dtflw`:

```python
# Notebook 
# /Repos/user@company.com/demo/main'

from dtflw import init_flow
from dtflw.io.azure import init_storage

storage = init_storage("account", "container", root_dir="analysis")
flow = init_flow(storage)
is_lazy = True

(
    flow.notebook("ingest_data")
        .input("sales_orders_raw", file_path="raw_data/sales_records_*.csv")
        .output("sales_orders_bronze")
        .run(is_lazy)
)

(
    flow.notebook("imprt_data")
        .input("sales_orders_bronze")
        .output("sales_orders_silver")
        .output("products_silver")
        .output("customers_silver")
        .run(is_lazy)
)

(
    flow.notebook("calculate_sales_stats")
        .args({
            "year" : "2022"
        })
        .input("sales_orders_silver")
        .input("customers_silver")
        .input("products_silver")
        .output("sales_stats_by_product_gold")
        .output("sales_stats_by_customer_gold")
        .run(is_lazy)
)

sales_stats_by_product_df = storage.read_table(_flow["sales_stats_by_product_gold"])
sales_stats_by_product_df.display()
```

Additionally, `dtflw` takes care of managing tables on a storage. It derives file paths from path of corresponding notebooks which produce them. For the example above, folders in an Azure blob container would look something like this:
```
https://account.blob.core.windows.net/container/

    raw_data/
        sales_records_2020.csv
        sales_records_2021.csv
        sales_records_2022.csv

    analysis/
        ingest_data/
            sales_orders_bronze.parquet/
        imprt_data/
            sales_orders_silver.parquet/
            products_silver.parquet/
            customers_silver.parquet/
        calculate_sales_stats/
            sales_stats_by_product_gold.parquet/
            sales_stats_by_customer_gold.parquet/
```

## [TODO] Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

What things you need to install the software and how to install them

```
Give examples
```

### Installing

A step by step series of examples that tell you how to get a development env running

Say what the step will be

```
Give the example
```

And repeat

```
until finished
```

End with an example of getting some data out of the system or using it for a little demo

## Changes

Please, refer to [CHANGES.md]() file to see previous changes made to this repo.

## Built With

`dtflw` is implemented using [dbutils](https://docs.databricks.com/dev-tools/databricks-utils.html).

## Contributing

Please refer to 
- [CODE_OF_CONDUCT.md]() for details on our code of conduct , and
- [CONTRIBUTING.md]() for details on how to contribute to the project.

## License

This project is licensed under the [BSD 3-Clause License](https://github.com/SoleyIo/dtflw/blob/main/LICENSE).
