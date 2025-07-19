# Ingest notebook

`03_ingest.ipynb` executes the ingestion logic for a single table. The notebook relies on widgets to pass in the table configuration.

1. `color` identifies the layer being processed.
2. `job_settings` supplies information about the target table and history options.

The table's JSON settings file may include a `dry_run` property. When `dry_run` is set to `"true"` the notebook prints the settings and exits without processing the table.

The notebook reads the settings JSON from `layer_*_<color>` and applies `apply_job_type` to expand any default options. Settings may specify a `pipeline_function` to run an entire pipeline, or separate `read_function`, `transform_function` and `write_function` for a step-by-step flow.

During execution the notebook performs the following steps:

- Prints the current job and table settings with `print_settings`.
- When the `dry_run` setting is `true`, processing stops after printing settings.
- Invokes the pipeline function or the individual read/transform/write functions.
- Creates a DQX bad records table when applicable.
- Builds a bad records table for bronze jobs if bad record files exist.
- Builds the file ingestion history table when `build_history` is enabled and the history schema is present. Column types are retained and versions that fail to load due to missing files are skipped.
