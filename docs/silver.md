# Silver layer configuration

This project divides silver table processing into **parallel** and **sequential** tasks.

## `requires` field

A silver table JSON file may include a `requires` key declaring dependencies on
other silver tables. The value can be a single table name or a list of table
names.

Example:

```json
{
  "job_type": "silver_standard_batch",
  "src_table_name": "edsm.bronze.example",
  "dst_table_name": "edsm.silver.example",
  "requires": ["systemsPopulated", "stations"]
}
```

Tables with a `requires` key are executed in the **sequential** silver loop.
They are sorted so that each table appears after all tables it requires. Tables
without a `requires` key run in the **parallel** silver loop.

## Workflow summary

1. `00_job_settings.ipynb` scans all `layer_02_silver/*.json` files.
2. Files without `requires` populate `silver_parallel`.
3. Files with `requires` populate `silver_sequential`, ordered by dependency.
4. `job-definition.yaml` defines two loops:
   - `silver_parallel_loop` (runs tasks concurrently)
   - `silver_sequential_loop` (runs tasks one at a time)

Dependencies are only evaluated among tables in `silver_sequential`.
If a sequential table depends on a parallel table, ensure the parallel
loop finishes first or coordinate via other mechanisms. This is handled
in the job definition. You will see that the parallel silver runs before
the sequential silver.
