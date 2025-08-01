# Drop schema tables notebook

`utilities/drop_schema_tables.ipynb` provides a simple helper to clean
out Delta history artifacts.

- Defines `drop_history_tables(spark, catalog, schema)` which lists all tables in the
  given catalog schema and collects their names.
- Drops every table that ends with `_file_version_history` or `_transaction_history`.
- The notebook calls the function for the `bronze` schema as an example.

Run this notebook whenever you need to remove leftover history tables from a
schema.
