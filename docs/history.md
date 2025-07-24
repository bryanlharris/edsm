# History notebook

`04_history.ipynb` builds the file ingestion history table for a single table.

1. `color` identifies the layer being processed.
2. `job_settings` supplies the table name and history options.
3. Reads the settings JSON and prints the resolved configuration.
4. Calls `build_and_merge_file_history` when the history schema exists.
5. Skips execution if no history schema is configured or the schema can't be found.

This notebook runs after bronze ingestion so history creation can happen alongside the silver processing.
