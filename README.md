# dtflw / <ins>d</ins>a<ins>t</ins>a<ins>fl</ins>o<ins>w</ins>

`dtflw` is a Python framework for building modular data pipelines based on [Databricks dbutils.notebook API](https://docs.databricks.com/notebooks/notebook-workflows.html). It was conceived with an intention to facilitate development and maintenance of Databricks data pipelines.

## Why `dtflw`
[Databricks](https://docs.databricks.com/notebooks/index.html) offers everything necessary to organize and manage code of data pipelines according to different requirements and tastes. Also, it does not impose any specific file structure on a repo of a pipeline neither regulates relationships between data (tables) and code transforming them (notebooks).

In general, such freedom is an advantage, but with a growing number of notebooks, variety of data and complexity of analysis logic
>_it gets laborious to work with a codebase of a pipeline while debugging, extending or refactoring it._

Among dozens of notebooks of a pipeline and thousands lines of code, `it is difficult to keep in mind which tables a notebooks requires to work and what tables it produces`. On the other side, when exploring tables (files) on a storage (e.g. Azure Blob, AWS S3), `it is unclear which code wrote those tables`.

The complexity rises even more when a team needs to `maintain many of such pipelines which are not structured according to a single uniform pattern`.

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
# /Repos/user@company.com/project/main'

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
    flow.notebook("import_data")
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

Additionally, `dtflw` manages file paths of output tables. It derives file paths of tables from path of corresponding notebooks which save them on a storage. For the example above, an Azure blob container would look something like this:
```
https://account.blob.core.windows.net/container/

    raw_data/
        sales_records_2020.csv
        sales_records_2021.csv
        sales_records_2022.csv

    analysis/
        project/
            ingest_data/
                sales_orders_bronze.parquet/
            import_data/
                sales_orders_silver.parquet/
                products_silver.parquet/
                customers_silver.parquet/
            calculate_sales_stats/
                sales_stats_by_product_gold.parquet/
                sales_stats_by_customer_gold.parquet/
```

## Getting Started

Clone the repo `git clone https://github.com/SoleyIo/dtflw.git`.

### Prerequisites

`dtflw` is tested with Python 3.8.*.

Install dependencies from the `install_requires` section in [setup.py](setup.py). You may want to to do that in a virtual environment and could use [virtualenv](https://pypi.org/project/virtualenv/) for that.

### Building

Build a `.whl` Python package `python setup.py sdist bdist_wheel`.

### Installing

As soon as you have a `.whl` Python package, [install it on a Databricks cluster](dtflw-0.0.8-py3-none-any.whl). 

`dtflw` is ready to be used.

## Changes

Please, refer to [the log of changes](CHANGES.md) made to this repo in every new version.

## Built With

`dtflw` is implemented using [dbutils](https://docs.databricks.com/dev-tools/databricks-utils.html).

## Contributing

Please refer to [our code of conduct](CODE_OF_CONDUCT.md) and to [our contributing guidelines](CONTRIBUTING.md) for details.

## License

This project is licensed under the [BSD 3-Clause License](LICENSE).
