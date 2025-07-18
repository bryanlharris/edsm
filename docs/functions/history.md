# `functions.history`

Utilities for capturing Delta transaction details and file lineage.

## `describe_and_filter_history`

Return a sorted list of Delta versions that were produced by `STREAMING UPDATE` or `MERGE` operations.

## `build_and_merge_file_history`

Create or update `<table>_file_ingestion_history` with new file paths for each
tracked version.  Each row stores the file path together with the transaction
details from `DESCRIBE HISTORY`.

## `transaction_history`

Alias for `build_and_merge_file_history` to preserve backwards compatibility.

