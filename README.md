# EDSM

This repository contains notebooks and utilities for ingesting data from the Elite Dangerous Star Map (EDSM) nightly dumps.

For MS Word documentation, see [here](https://github.com/bryanlharris/Documentation). This could be easier to read but not as detailed.

If you just want to get started building a job like this one, read the first doc.

* docs/procedures/build_pipeline.md - Build a pipeline based on this EDSM pipeline.
* docs/bronze.md - Overview of the bronze layer.
* docs/silver.md - Overview of how silver data is ingested, transformed and written.
* docs/table_dependencies.md - Silver table dependencies and execution order.
* docs/ingest.md - Explanation of the ingest notebook.
* docs/sampling.md - Information on generating sample data.
* docs/job-definition.md - Overview of the job-definition YAML.
* docs/job_settings.md - Details about preparing configuration for each task.
* docs/custom.md - Creating and using custom transforms.
* docs/history.md - Notebook for building file ingestion history.
* docs/requirements.md - Overview of project requirements and capabilities.
* docs/functions/config.md - Constants shared across notebooks.
* docs/functions/utility.md - Utility helper functions.
* docs/functions/read.md - Functions that read data sources.
* docs/functions/write.md - Functions that write and merge tables.
* docs/functions/history.md - Delta transaction history utilities.
* docs/functions/quality.md - Data quality helpers.
* docs/functions/dq_checks.md - Custom DQX check functions.
* docs/functions/sanity.md - Sanity helper functions.
* docs/functions/rescue.md - Table rescue utilities.
* docs/functions/transform.md - Transformation helpers.
* docs/utilities/downloader.md - Details about fetching nightly dumps.
* docs/utilities/profile_silver_table.md - Notebook for generating DQX rules from a silver table.
* docs/utilities/drop_schema_tables.md - Notebook for removing history tables from a schema.
* docs/utilities/migrate_history_tables.md - Convert and remove old history tables.
* docs/utilities/add_history_ingest_time.md - Notebook for adding `ingest_time` to history tables.
* docs/utilities/drop_rescued_data.md - Notebook for removing `_rescued_data` columns from all tables.
* docs/utilities/restore_table_version_dashboard.md - Databricks SQL dashboard for restoring table versions.
* docs/utilities/uploader.md - Script for uploading notebooks to the workspace (excludes `.git`).
* docs/delta-sharing.md - Sharing tables over Delta Sharing.
