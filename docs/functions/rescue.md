# `functions.rescue`

Helpers for rebuilding silver and gold tables when streaming jobs need to be recovered.

## `rescue_silver_table`

Replay a silver table from its source history.  The `mode` argument determines
whether batches are rebuilt by `derived_ingest_time` (`"timestamp"`) or by Delta
version (`"versionAsOf"`).  The function deletes the destination table and its
checkpoint directory, then replays each batch through the configured
transforms and upsert logic.

## `rescue_silver_table_timestamp`

Convenience wrapper that calls `rescue_silver_table` with `"timestamp"` mode for
backwards compatibility.

## `rescue_silver_table_versionAsOf`

Convenience wrapper that calls `rescue_silver_table` with `"versionAsOf"` mode.

## `rescue_gold_table`

Rebuild a gold table by iterating over every Delta version of the source table.
Each snapshot is transformed and written, recreating the destination from
scratch.
