# Add ingest_time column

`utilities/add_history_ingest_time.ipynb` adds an `ingest_time` column to each table in a history schema.

- Defines `add_ingest_time_column(schema, spark)` which lists all tables in the schema.
- Adds a new `TIMESTAMP` column named `ingest_time` when missing and prints the results.
- The notebook calls the function for `edsm.history` as an example.
