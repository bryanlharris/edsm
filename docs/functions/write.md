# `functions.write`

Utility functions for writing and merging data in Delta tables. Each
function accepts a DataFrame, a settings dictionary and a Spark session.

## `overwrite_table`

Overwrite the destination table with the provided DataFrame.

## `stream_write_table`

Write a streaming DataFrame directly to a Delta table. Streaming options
are read from `writeStreamOptions` in the settings. The function returns
the ``StreamingQuery`` and blocks until the stream completes.

## `stream_upsert_table`

Apply an upsert function to each micro-batch of a streaming query. The
upsert function name is supplied in the settings and loaded with
`get_function`. The returned ``StreamingQuery`` is awaited so the
function blocks until streaming finishes.

## `_simple_merge`

Internal helper implementing the standard merge logic for non-SCD2
upserts. Rows are deduplicated by the business key and merged into the
destination table when values change.

## `_scd2_upsert`

Internal helper for slowly changing dimension type 2 (SCD2) merges. The
latest record for each business key is identified using the ingest time
column before updating or inserting rows with validity ranges.

## `upsert_table`

Generic entry point used by both batch and streaming jobs. It delegates
to `_simple_merge` or `_scd2_upsert` based on the `scd2` flag and can
create the destination table during the first streaming micro-batch.

## `microbatch_upsert_fn`

Return a function suitable for `foreachBatch` that performs standard
upserts using `upsert_table`.

## `microbatch_upsert_scd2_fn`

Return a `foreachBatch` function that performs SCD2 upserts. The helper
prints micro-batch information before calling `upsert_table`.

## `batch_upsert_scd2`

Thin wrapper to perform an SCD2 merge in batch mode using
`upsert_table`.

## `write_upsert_snapshot`

Write the most recent row per business key into a snapshot table. The
function uses a merge statement to update or insert rows based on the
business key.

