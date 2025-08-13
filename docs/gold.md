# Gold layer

Gold jobs build the final curated tables or trigger custom SQL notebooks.

Supported job types (as defined in `functions.config.JOB_TYPE_MAP`):

- `gold_standard_batch` – batch reads a silver table and writes the final
  snapshot to the gold layer using the standard transform. Use this for
  straightforward table-to-table promotion when custom SQL is not required.
- `gold_sql_notebook` – runs a Databricks SQL notebook via
  `functions.sql.run_sql_notebook`. Choose this when the transformation is
  expressed as custom SQL code.

## `gold_standard_batch`

To promote a silver table to gold with the standard pipeline, create a JSON
config in `layer_03_gold` (or subdirectories) with `simple_settings` set to
`true` and `job_type` set to `gold_standard_batch`. Provide the source and
destination tables along with the business key columns used for merging.

Example configuration:

```json
{
  "simple_settings": "true",
  "job_type": "gold_standard_batch",
  "src_table_name": "edsm.silver.example",
  "dst_table_name": "edsm.gold.example",
  "business_key": ["id"]
}
```

## `gold_sql_notebook`

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
