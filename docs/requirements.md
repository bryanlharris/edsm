# Project Requirements Met

This project implements a full Databricks pipeline for ingesting the Elite Dangerous Star Map (EDSM) dumps and refining them into curated tables. The following requirements are satisfied:

- **Ingest raw data with Auto Loader.** Bronze tables read the EDSM export files using Databricks Auto Loader and write Delta tables. Settings files under `layer_01_bronze` normally use the `bronze_standard_streaming` job type.
- **Refine bronze data into silver tables.** Silver configuration files define job types such as `silver_standard_streaming` or `silver_scd2_batch` to transform and merge data into curated Delta tables.
- **Simplified silver processing.** Silver tables run independently unless they declare upstream dependencies through an optional `requires` field.
- **Run ingestion notebooks per table.** `03_ingest.ipynb` executes the selected pipeline, prints settings, runs any DQX checks and creates bad record tables when needed.
- **Track file ingestion history.** `04_history.ipynb` builds `<table>_file_ingestion_history` tables storing Delta transaction details and file paths.
- **Organize tasks with a job definition.** `job-definition.yaml` triggers the workflow when new files arrive and coordinates downloader, bronze, history, silver and gold loops.
- **Support sampling and data quality.** The `sample_table` helper creates deterministic or random subsets and utilities exist for profiling tables and generating DQX rules.
- **Publish data through Delta Sharing.** Instructions are provided for creating shares and reading them with the Delta Sharing client.
- **Allow custom transforms.** Additional logic can be implemented in `functions/custom.py` and referenced in settings files.
- **Provide utility notebooks.** Scripts are included for fetching nightly dumps, dropping `_rescued_data` columns, migrating history tables, inspecting checkpoints and other maintenance tasks.


