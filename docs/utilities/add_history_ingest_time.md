# Add ingest_time column

`utilities/add_history_ingest_time.ipynb` adds an `ingest_time` column to each table in a history schema. A second cell can update existing history tables with a `row_hash` column.

- Defines `add_ingest_time_column(schema, spark)` which lists all tables in the schema.
- Adds a new `TIMESTAMP` column named `ingest_time` when missing and prints the results.
- Defines `add_row_hash_column(schema, spark)` to create the new `row_hash` column when missing.
- The notebook calls both functions for `edsm.history` as an example.
