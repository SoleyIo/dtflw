# dtflw / dataflow

`dtflw` is a Python framework for building modular data pipelines based on [Databricks dbutils.notebook API](https://docs.databricks.com/notebooks/notebook-workflows.html). It was conceived with an intention to facilitate development and maintenance of Databricks data pipelines.

[Databricks](https://docs.databricks.com/notebooks/index.html) offers all necessary to nicely organize and manage code of a data pipeline according to different requirements and tastes. Also, it does not impose any specific file structure on a repo of a pipeline neither strinctly regulates relationships between data (tables in this context) and transformation code.

In general, such freedom is an advantage, but with an increasing 
- complexity of data pipelines (e.g. number of loc, notebooks, tables),
- overal number of production pipelines which require regular maintenance

>_it gets difficult to work with a codebase to fix a bug, introduce an extension or refactor it._

> TODO: what such difficulties are caused by?

`dtflw` embodies practices which aim at mitigating such difficulties.

It builds around a simple dataflow model according to which 
> 1. A notebook consumes tables (inputs), produces tables (outputs) and may require additional arguments to work.  
> 2. Such notebooks are chained in a data pipeline.
> 3. ?

Here is an example of a data pipeline which is built with `dtflw`:
```
from dtflw import init_flow
from dtflw.io.azure imprt AzureStorage

storage = AzureStorage("account", "container", spark, dbutils)
flow = init_flow(storage)

(
    flow.notebook("ingest_data")
        .output("sales_orders_bronze")
        .run()
)

(
    flow.notebook("imprt_data")
        .input("sales_orders_bronze")
        .output("sales_orders_silver")
        .output("products_silver")
        .output("customers_silver")
        .run()
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
        .run()
)
```

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
