# `functions.history`

Utilities for capturing Delta transaction details and file lineage.

## `describe_and_filter_history`

Return a sorted list of Delta versions that were produced by `STREAMING UPDATE`,
`STREAMING MERGE`, `MERGE`, `WRITE`, `UPDATE`, `DELETE` or `RESTORE` operations.

## `build_and_merge_file_history`

Create or update `<table>_file_ingestion_history` with new file paths for each
tracked version. Each row stores the file path together with the full
transaction details from `DESCRIBE HISTORY` plus an ``ingest_time`` column
recording when the row was inserted. A hash of ``file_path``, ``table_name``,
``timestamp`` and ``ingest_time`` is stored in ``row_hash`` so duplicate rows are
ignored on merge. Pass ``dst_table_name`` to override the destination table name;
otherwise the name is derived from ``full_table_name`` and ``history_schema``.
This prevents duplicates if the Delta table version is reset (for example after
cloning). Column types are preserved so struct and map fields remain intact.
Versions that cannot be read because a referenced file is missing are skipped.


