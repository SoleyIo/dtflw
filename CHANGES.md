# Changes
_All notable changes to the codebase are documented in this file._

## [0.6.6] - 17.08.2023
- Added `dtflw.storage.fs.FileStorageBase.write_table` method.
- Added a demo project showing how to abstract from a specific storage in notebooks.

## [0.6.5] - 10.08.2023
- Fixed: some functions of `dtflw.databricks` used to catch and filter for `java.util.NoSuchElementException`.
Filtering has been removed since it may be a different exception class.

## [0.6.4] - 09.04.2023
- Improve validation, skip reading output table if table expected_columns are not set.

## [0.6.3] - 09.04.2023
- Fixed bug with defining params for the act method of the notebook plugin.

## [0.6.2] - 29.03.2023
- Add extras_require section in `setup.py` for the extra packages.

## [0.6.1] - 25.03.2023
- Fixed bug with multiple NotebookPlugins installed.

## [0.6.0] - 12.03.2023
- Added `dtflw.storage.DbfsStorage`.
- Added a demo project. See `demos/dtflw_intro`.

## [0.5.1] - 24.02.2023
- Prepared the package for PyPi.

## [0.5.0] - 17.02.2023
- Added `dtflw.databricks.is_job_interactive`.

## [0.4.0] - 17.02.2023
- Added `LazyNotebook.share_arguments`.
- Removed `dtflw.events`.

## [0.3.0] - 14.02.2023
- Added functions `init_args`, `init_inputs` and `init_outputs` for initializing arguments in a callee notebook
using `dbutils.widgets` API.
- Updated `README.md`.

## [0.2.0] - 02.02.2023
- Module `dtflw.io.storage` renamed to `dtflw.storage.fs`.

## [0.1.0] - 02.02.2023
> We open source `dtflw` framework to share our experience of building Databricks data pipelines.  
> We think that it might be found useful and inspiring for others and, we hope that it will serve them well.
>
> Its initial version is `0.1.0` denoting (major digit is `0`) that its public API may still change any time and should not be considered stable.

## [0.0.11] - 30.01.2023
- Added verbosity control for `DefaultLogger`.

## [0.0.10] - 23.01.2023
- Added `dtflw.display.DefaultDisplay` service for interacting with a user in a notebook.

## [0.0.9] - 19.01.2023
- Added `dtflw.init_flow` and `dtflw.io.azure.init_storage` factory functions.
- Updated the README and documents.

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
