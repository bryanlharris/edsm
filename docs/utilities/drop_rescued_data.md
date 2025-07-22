# Drop `_rescued_data` columns

`utilities/drop_rescued_data.ipynb` removes the `_rescued_data` column from every table in the `edsm` catalog.

- Defines `drop_rescued_data_columns(spark, catalog)` which lists all schemas and tables in the catalog.
- Skips system schemas (`information_schema` and `sys`) using a simple list.
- Skips views so only tables are processed.
- Sets the Delta column mapping mode to `name` before dropping the column.
- Drops `_rescued_data` when present and logs the action.
- The notebook calls the function for the `edsm` catalog as an example.
