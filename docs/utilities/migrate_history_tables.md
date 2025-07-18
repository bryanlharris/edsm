# Migrate old history tables

`utilities/migrate_history_tables.ipynb` converts `_file_version_history` and `_transaction_history` tables into the new `_file_ingestion_history` format.

- Defines `migrate_history(schema, spark)` which scans the schema for old history tables.
- Joins file and transaction history on version, merging the result into `<table>_file_ingestion_history`.
- When the merge succeeds the old tables are dropped.
- The notebook calls the function for `edsm.history` as an example.
