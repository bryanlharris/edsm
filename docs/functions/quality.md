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

## `create_dqx_bad_records_table`

Run DQX checks and materialize failing rows to a Delta table named
`<dst_table_name>_dqx_bad_records`.  Existing tables are removed when no
failures are found.  If the table exists after processing, an exception is
raised and the cleaned DataFrame is returned.
