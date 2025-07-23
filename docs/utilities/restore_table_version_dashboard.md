# Restore table version dashboard

The `SQL_restore_table_version.ipynb` notebook provides a Databricks SQL interface for restoring Delta tables to a previous version.

- `SHOW TABLES` lists tables under the chosen `schema`.
- `DESCRIBE HISTORY` displays available versions for the selected `table`.
- The final query runs `RESTORE TABLE` using the specified `version`.

Fill in the widget values for `schema`, `table` and `version` then execute the queries in order.
