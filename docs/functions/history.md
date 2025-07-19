# `functions.history`

Utilities for capturing Delta transaction details and file lineage.

## `describe_and_filter_history`

Return a sorted list of Delta versions that were produced by `STREAMING UPDATE` or `MERGE` operations.

## `build_and_merge_file_history`

Create or update `<table>_file_ingestion_history` with new file paths for each
tracked version. Each row stores the file path together with the full
transaction details from `DESCRIBE HISTORY` plus an ``ingest_time`` column
recording when the row was inserted. A ``row_hash`` column hashes the file path
and history columns (excluding ``version``) so rows can be uniquely matched even
if the Delta table's version counter is reset. Column types are preserved so
struct and map fields remain intact. Versions that cannot be read because a
referenced file is missing are skipped.

## `transaction_history`

Alias for `build_and_merge_file_history` to preserve backwards compatibility.

