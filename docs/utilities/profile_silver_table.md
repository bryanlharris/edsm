# Profile silver table

The `profile_silver_table.ipynb` notebook builds data quality rules for an existing silver table.

## `utilities/profile_silver_table.ipynb`

- Lists every settings file under `layer_02_silver` in a dropdown widget.
- Installs `databricks-labs-dqx` and restarts the Python kernel.
- Reads the selected table name from the widget and applies defaults with `apply_job_type`.
- Loads the table from Spark and profiles each column using `DQProfiler`.
- Generates a JSON array of checks with `DQGenerator` and displays it with a copy button.
