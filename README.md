# EDSM

This repository contains notebooks and utilities for ingesting data from the Elite Dangerous Star Map (EDSM) nightly dumps.

For documentation, see [here](https://github.com/bryanlharris/Documentation).

See [docs/silver.md](docs/silver.md) for details about configuring sequential and parallel silver tasks.
See [docs/bronze.md](docs/bronze.md) for an overview of the bronze layer.
See [docs/custom.md](docs/custom.md) for creating and using custom transforms.
See [docs/job-definition.md](docs/job-definition.md) for an overview of the job-definition YAML.
See [docs/job_settings.md](docs/job_settings.md) for details about preparing configuration for each task.
See [docs/ingest.md](docs/ingest.md) for an explanation of the ingest notebook.
See [docs/sampling.md](docs/sampling.md) for information on generating sample data.
See [docs/functions/config.md](docs/functions/config.md) for constants shared across notebooks.
See [docs/functions/utility.md](docs/functions/utility.md) for utility helper functions.
See [docs/functions/read.md](docs/functions/read.md) for functions that read data sources.
See [docs/functions/write.md](docs/functions/write.md) for functions that write and merge tables.
See [docs/functions/history.md](docs/functions/history.md) for Delta transaction history utilities.
See [docs/functions/quality.md](docs/functions/quality.md) for data quality helpers.
See [docs/functions/rescue.md](docs/functions/rescue.md) for table rescue utilities.
See [docs/functions/dq_checks.md](docs/functions/dq_checks.md) for custom DQX check functions.
