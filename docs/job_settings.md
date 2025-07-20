# Job settings notebook

`00_job_settings.ipynb` prepares configuration values for each task in the job. It scans all JSON files under `layer_*_<color>` and groups them by color.

For bronze and gold tables every file is loaded and the following data is written to `job_settings[color]`:

- `table` – the table name derived from the file name
- `history` – dictionary with `build_history`, `history_schema` and `full_table_name`

Silver files are divided into parallel and sequential lists based on the presence of a `requires` field.

- Tables **without** `requires` populate `silver_parallel`.
- Tables **with** `requires` populate `silver_sequential` after sorting by dependency via `functions.utility.sort_by_dependency`.

Each section of `job_settings` is stored in a Databricks task value so downstream notebooks can access the configuration.
Finally the notebook runs several sanity checks:

1. `validate_settings` verifies the settings are well formed and warns if the
   S3 root paths are missing a trailing `/`.
2. `initialize_schemas_and_volumes` ensures catalogs and volumes exist and
   prints a warning when it must create the configured history schema.
3. `initialize_empty_tables` creates any empty destination tables.
