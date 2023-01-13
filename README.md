# dtflw / dataflow

`dtflw` is a Python framework for building modular data pipelines based on [Databricks dbutils.notebook API](https://docs.databricks.com/notebooks/notebook-workflows.html). It was conceived with an intention to facilitate development and maintenance of Databricks data pipelines.

[Databricks](https://docs.databricks.com/notebooks/index.html) offers everything necessary to organize and manage code of data pipelines according to different requirements and tastes. Also, it does not impose any specific file structure on a repo of a pipeline neither regulates relationships between data (tables) and code transforming them (notebooks).

In general, such freedom is an advantage, but with increasing complexity of a pipeline (e.g. growing number of loc, notebooks, volume and variety of data)
>_it gets difficult and laborious to work with a codebase of a pipeline while debugging, extending or refactoring it._

Together with a growing overall number of such pipelines, consequently grow efforts of their development and maintenance.

This project identifies `implicit relationships between data and code in a pipeline` as the main reason of the problem described above. In other words:
> - Among dozens of notebooks of a pipeline, it is difficult to keep in mind which table a notebooks requires to work and what tables it produces.
> - From the other side: exploring tables (files) on a storage (e.g. Azure Blob, AWS S3), it may be difficult to relate which notebook produced it.

`dtflw` addresses the problem by building on a simple dataflow model:
> Each notebook of a pipeline
> 1. (may) consume tables (inputs), 
> 2. (may) produce tables (outputs),
> 3. and may declare additional arguments it requires to run.  
>
> Thus, a pipeline is a sequence of notebooks chained by input-output tables.

In such a way, `dtflw` `makes relationships between tables and notebooks explicit`.

Here is an example of a Databricks pipeline built using `dtflw`:

.. code-block:: python

from dtflw import init_flow
from dtflw.io.azure import AzureStorage

storage = AzureStorage("account", "container", spark, dbutils)
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


`dtflw` is implemented using `dbutils`, `dbutils.notebooks` and `dbutils.widgets` APIs provided by Databricks and can be easily mixed with them.

## Getting Started

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

## Running the tests

Explain how to run the automated tests for this system

### Break down into end to end tests

Explain what these tests test and why

```
Give an example
```

### And coding style tests

Explain what these tests test and why

```
Give an example
```

## Deployment

Add additional notes about how to deploy this on a live system

## Built With

* [Dropwizard](http://www.dropwizard.io/1.0.2/docs/) - The web framework used
* [Maven](https://maven.apache.org/) - Dependency Management
* [ROME](https://rometools.github.io/rome/) - Used to generate RSS Feeds

## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/your/project/tags). 

## Authors

* **Billie Thompson** - *Initial work* - [PurpleBooth](https://github.com/PurpleBooth)

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This project is licensed under the [BSD 3-Clause License](https://github.com/SoleyIo/dtflw/blob/main/LICENSE).

## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc
