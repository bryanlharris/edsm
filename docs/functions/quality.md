# `functions.quality`

Helpers for running data quality rules with the Databricks `databricks-labs-dqx` library.
The functions lazily import DQX so that the module can be imported without
requiring PySpark.

## `apply_dqx_checks`

Validate a DataFrame using the checks listed in `settings['dqx_checks']`.
Custom row-level checks from `functions.dq_checks` are registered when
possible.  Returns the pair `(good_df, bad_df)` with rows split by the rule
results.  If no checks are supplied, the original DataFrame and an empty one
with the same schema are returned.

## `count_records`

Return the number of rows in a DataFrame.  Streaming inputs are written to an
in-memory table with a unique checkpoint so that the count can be collected
synchronously.  The optional `checkpoint_location` controls where temporary
checkpoints are stored when counting streaming records.

## `create_dqx_bad_records_table`

Run DQX checks and materialize failing rows to a Delta table named
`<dst_table_name>_dqx_bad_records`.  Failing rows are counted first and the
table is created with `create_table_if_not_exists` only when at least one
record fails.  Any existing table is removed when no failures are found.  If
the table remains after processing, an exception is raised and the cleaned
DataFrame is returned.
