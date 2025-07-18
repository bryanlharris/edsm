# `functions.history`

Utilities for capturing Delta transaction details and file lineage.

## `describe_and_filter_history`

Return a sorted list of Delta versions that were produced by `STREAMING UPDATE` or `MERGE` operations.

## `build_and_merge_file_history`

Create or update `<table>_file_version_history` with new file paths introduced in each tracked version.

## `transaction_history`

Persist the output of `DESCRIBE HISTORY` in `<table>_transaction_history` for auditing purposes.

