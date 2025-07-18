# Drop history tables notebook

`utilities/drop_history_tables.ipynb` provides a simple helper to clean
out Delta history artifacts.

- Defines `drop_history_tables(spark, schema)` which lists all tables in the
  given catalog schema and collects their names.
- Drops every table that ends with `_file_ingestion_history`.
- The notebook calls the function for the `bronze` schema as an example.

Run this notebook whenever you need to remove leftover history tables from a
schema.
