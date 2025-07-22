# Job definition

This document describes the [`job-definition.yaml`](../job-definition.yaml) file.

The job is named **edsm** and is triggered when new files arrive under `/Volumes/edsm/bronze/landing/`.  When triggered, the following tasks run:

- **downloader** – runs the `utilities/downloader` notebook to fetch the nightly dump.
- **job_settings** – loads job configuration from the `00_job_settings` notebook and passes settings to downstream tasks.
- **bronze_loop** – iterates over the `bronze` job settings, launching the `03_ingest` notebook for each table with the parameter `color` set to `bronze`.
- **history_loop** – builds file ingestion history tables using the `04_history` notebook.
- **silver_parallel_loop** – processes tables listed under `silver_parallel` in parallel using the `03_ingest` notebook with `color` set to `silver`.
- **silver_sequential_loop** – runs the remaining silver tasks one at a time from `silver_sequential`.
- **silver_samples_loop** – executes any sampling jobs defined under `silver_samples` after the sequential tasks complete.
- **gold_loop** – executes the final gold layer jobs defined in `gold`.

Each task references notebooks in the Databricks workspace and has retry settings disabled for auto optimization.

The job is queued and associated with a budget policy and a default environment.

Job-definition changes for this additional loop are performed separately.
