# Gold layer

Gold jobs build the final curated tables or trigger custom SQL notebooks.

To run a SQL notebook as part of the gold pipeline, create a JSON config in
`layer_03_gold` (or subdirectories) with `simple_settings` set to `true` and
`job_type` set to `gold_sql_notebook`. The config must also provide the
Databricks workspace path to the notebook via `notebook_path`.

Example configuration:

```json
{
  "simple_settings": "true",
  "job_type": "gold_sql_notebook",
  "notebook_path": "/Workspace/Shared/example_sql_notebook"
}
```

During ingestion, the `run_sql_notebook` pipeline executes the referenced
notebook using `dbutils.notebook.run`.
