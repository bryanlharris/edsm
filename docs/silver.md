# Silver layer

Silver tables refine the cleaned bronze data into curated Delta tables. Each configuration file under `layer_02_silver` describes one table and usually sets `simple_settings` to `true` with an appropriate `job_type`.

## Standard job types

These are the possible values for `job_type` when `simple_settings` is set to `true` in the json file.
Silver jobs read from existing tables and apply one of the predefined pipelines:

- **silver_scd2_streaming** – streams from a source table, transforms the data with `silver_scd2_transform` and upserts using `stream_upsert_table`.
- **silver_upsert_streaming** – similar to the above but uses a standard transform and simple microbatch upserts.
- **silver_standard_streaming** – reads from a table, transforms the rows and writes the stream without merges.
- **silver_scd2_batch** – batch reads and upserts with SCD2 semantics.
- **silver_standard_batch** – batch reads and writes a snapshot table.

These pipelines rely on helper functions from `functions.read`, `functions.transform` and `functions.write` just like the bronze layer.

## Transform details

`silver_standard_transform` cleans column names, casts data types and records the source file path. When `use_row_hash` is enabled the transform adds a stable hash over the surrogate key columns. `silver_scd2_transform` extends this logic with SCD2 tracking fields such as `valid_from` and `current_flag`.

## Writing results

Batch jobs call `write_upsert_snapshot` or `batch_upsert_scd2` depending on the job type. Streaming jobs use `stream_write_table` or `stream_upsert_table` with a microbatch helper. These functions merge new rows into the destination table or overwrite it for snapshot outputs.
