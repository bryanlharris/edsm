# `functions.utility`

This module provides helper functions used across the ingestion notebooks. They handle HTML rendering of settings, manage job type defaults, create tables and volumes, and inspect streaming checkpoints.

## `print_settings`

Display job and table settings with copy buttons. Uses `displayHTML` when available and falls back to plain printing outside Databricks.

## `_merge_dicts`

Internal helper that recursively combines dictionaries, giving precedence to override values.

## `apply_job_type`

When `simple_settings` is `true`, expand `job_type` into explicit function names from `JOB_TYPE_MAP`. For `bronze_standard_streaming` it also derives landing and utility paths from `dst_table_name`. Silver and gold job types add history and streaming defaults as needed.

## `sort_by_dependency`

Return tables ordered so all dependencies come first. Raises an exception if a cycle is detected.

## `get_function`

Import and return a callable from a dotted path such as `functions.read.stream_read_cloudfiles`.

## `create_table_if_not_exists`

Create an empty Delta table using the DataFrame schema when the destination table is missing.

## `create_schema_if_not_exists`

Ensure the schema exists in the catalog and print a message when it is created.

## `schema_exists` and `catalog_exists`

Return `True` when the given schema or catalog already exists.

## `volume_exists` and `create_volume_if_not_exists`

Check for an external volume and create it with the expected S3 location if needed.

## `truncate_table_if_exists`

Truncate a table when it already exists.

## `inspect_checkpoint_folder`

Print the mapping between streaming batch IDs and bronze versions by reading Delta checkpoint files.

## `create_bad_records_table`

Create `<dst_table_name>_bad_records` from JSON files in `badRecordsPath`. If no files are present the table is dropped. An exception is raised if the table exists after creation, signalling that bad records were found.

