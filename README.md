# dtflw / <ins>d</ins>a<ins>t</ins>a<ins>fl</ins>o<ins>w</ins>

![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/SoleyIo/dtflw/test-and-report.yml)
![badge](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/skndrg/559a8785afeae906021482849a3b6762/raw/7504f308ddf48ee752ea1367270fa7f04dce5c43/dtflw-coverage-badge.json)
![GitHub release (latest by date)](https://img.shields.io/github/v/release/SoleyIo/dtflw)
![GitHub](https://img.shields.io/github/license/SoleyIo/dtflw)

`dtflw` is a Python framework for building modular data pipelines based on [Databricks dbutils.notebook API](https://docs.databricks.com/notebooks/notebook-workflows.html). It was conceived with an intention to facilitate development and maintenance of Databricks data pipelines.

## Why `dtflw`?
[Databricks](https://docs.databricks.com/notebooks/index.html) offers everything necessary to organize and manage code of data pipelines according to different requirements and tastes. Also, it does not impose any specific  structure on a repo of a pipeline neither regulates relationships between data (tables) and code transforming them (notebooks).

In general, such freedom is an advantage, but with a growing number of notebooks, variety of data and complexity of analysis logic
>it gets laborious to work with a codebase of a pipeline while debugging, extending or refactoring it.

Among dozens of notebooks of a pipeline and thousands lines of code, 
> it is difficult to keep in mind which tables a notebook requires and what tables it produces. 

On the other side, when exploring tables (files) on a storage (e.g. Azure Blob, AWS S3), 
>it is unclear which code produced those tables.

The complexity rises even more when a team needs to 
>maintain numerous pipelines, each structured in its own way.

## How does `dtflw` work?
This project identifies _implicit relationships between data and code in a pipeline_ as the main reason for increasing complexity.

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
from dtflw.storage.azure import init_storage

storage = init_storage("account", "container", root_dir="analysis")
flow = init_flow(storage)
is_lazy = True

(
    flow.notebook("ingest_data")
        .input("SalesRecordsRaw", file_path="raw_data/sales_records_*.csv")
        # Bronze
        .output("SalesRecords")
        .run(is_lazy)
)

(
    flow.notebook("import_data")
        .input("SalesRecords")
        # Silver
        .output("SalesOrders")
        .output("Products")
        .output("Customers")
        .run(is_lazy)
)

(
    flow.notebook("calculate_sales_stats")
        .args({
            "year" : "2022"
        })
        .input("SalesOrders")
        .input("Customers")
        .input("Products")
        # Gold
        .output("SalesStatsPerProduct")
        .output("SalesStatsPerCustomer")
        .run(is_lazy)
)

storage.read_table(_flow["SalesStatsPerProduct"]).display()
```

File paths of input and output tables are passed to a callee notebook as arguments. [dbutils.widgets API](https://docs.databricks.com/notebooks/widgets.htm) is used to fetch values passed at runtime.

```python
# Notebook 
# /Repos/user@company.com/project/calculate_sales_stats'
from dtflw import init_args, init_inputs, init_outputs

args = init_args("year")
inputs = init_inputs("SalesOrders", "Customers", "Products")
outputs = init_outputs("SalesStatsPerProduct", "SalesStatsPerCustomer")

# Load inputs
sales_orders_df = spark.read.parquet(inputs["SalesOrders"].value)
# ...

# Save outputs
sales_stats_per_product_df.write.mode("overwrite")\
    .parquet(outputs["SalesStatsPerProduct"].value)
# ...
```

Additionally, `dtflw` takes care of constructing file paths for output tables. 
> It derives file paths of outputs from a path of corresponding notebook which save them. 

For the example above, an Azure blob container would look something like this:
```
https://account.blob.core.windows.net/container/

    raw_data/
        sales_records_2020.csv
        sales_records_2021.csv
        sales_records_2022.csv

    analysis/
        project/
            ingest_data/
                SalesRecords.parquet/
            import_data/
                SalesOrders.parquet/
                Products.parquet/
                Customers.parquet/
            calculate_sales_stats/
                SalesStatsPerProduct.parquet/
                SalesStatsPerCustomer.parquet/
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

Please, refer to [the log of changes](CHANGES.md). Here we record all notable changes made to the codebase in every version.

## Built With

`dtflw` is implemented using [dbutils](https://docs.databricks.com/dev-tools/databricks-utils.html).

## Contributing

Please refer to [our code of conduct](CODE_OF_CONDUCT.md) and to [our contributing guidelines](CONTRIBUTING.md) for details.

## License

This project is licensed under the [BSD 3-Clause License](LICENSE).
