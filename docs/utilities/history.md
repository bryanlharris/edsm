# History notebook

`utilities/history.ipynb` builds the file ingestion history table for a single bronze table.

1. Searches `layer_01_bronze` for settings files and lets you select a `table` widget.
2. Loads the table's JSON to read `dst_table_name` and `history_schema`.
3. Prints these settings for reference.
4. When the history schema exists, calls `build_and_merge_file_history` from `functions.history`. Column types match `DESCRIBE HISTORY` and versions with missing files are skipped automatically.
5. Skips execution if no history schema is configured or the schema can't be found.

Use this notebook to recreate history tables after backfilling or schema changes.
