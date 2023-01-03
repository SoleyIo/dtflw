# Changelog
*All notable changes to this package are documented in this file.*

## [0.0.8] - 03.01.2023
- Replaced `Runtime` with `PipelineState`. Renamed `LazyNotebook.collect_args` to `LazyNotebook.collect_arguments`.

## [0.0.7] - 02.01.2023
- Added `flow`, `plugin`, `flow_context`, `input_table`, `output_table`, `lazy_notebook`.

## [0.0.6] - 26.12.2022
- Refactored the unit tests to remove a need to run a cluster and to rely on an Azure blob container.

## [0.0.5] - 21.12.2022
- Added `assertions`, `databricks`, `events`, `logger`, `runtime`, `tables_repo`.

## [0.0.4] - 06.12.2022
- Removed version restriction on `setuptools`.

## [0.0.3] - 18.11.2022
- Added info to `setup.py`.

## [0.0.2] - 09.11.2022
- Added a new function `dtflw.databricks.get_path_relative_to_project_dir`.

## [0.0.1] - 07.11.2022
- Initialized the `dtflw` repo. 
- Added `dtflw.io`. 
- Set up unittest and a build definition.