# EDSM

This repository contains notebooks and utilities for ingesting data from the Elite Dangerous Star Map (EDSM) nightly dumps.

For documentation, see [here](https://github.com/bryanlharris/Documentation).

* [docs/bronze.md](docs/bronze.md) - Overview of the bronze layer.
* [docs/silver.md](docs/silver.md) - Details about configuring sequential and parallel silver tasks.
* [docs/ingest.md](docs/ingest.md) - Explanation of the ingest notebook.
* [docs/sampling.md](docs/sampling.md) - Information on generating sample data.
* [docs/job-definition.md](docs/job-definition.md) - Overview of the job-definition YAML.
* [docs/job_settings.md](docs/job_settings.md) - Details about preparing configuration for each task.
* [docs/custom.md](docs/custom.md) - Creating and using custom transforms.
* [docs/functions/config.md](docs/functions/config.md) - Constants shared across notebooks.
* [docs/functions/utility.md](docs/functions/utility.md) - Utility helper functions.
* [docs/functions/read.md](docs/functions/read.md) - Functions that read data sources.
* [docs/functions/write.md](docs/functions/write.md) - Functions that write and merge tables.
* [docs/functions/history.md](docs/functions/history.md) - Delta transaction history utilities.
* [docs/functions/quality.md](docs/functions/quality.md) - Data quality helpers.
* [docs/functions/dq_checks.md](docs/functions/dq_checks.md) - Custom DQX check functions.
* [docs/functions/sanity.md](docs/functions/sanity.md) - Sanity helper functions.
* [docs/functions/rescue.md](docs/functions/rescue.md) - Table rescue utilities.
* [docs/utilities/downloader.md](docs/utilities/downloader.md) - Details about fetching nightly dumps.
