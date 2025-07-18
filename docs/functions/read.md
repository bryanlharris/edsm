# `functions.read`

Utilities for reading data sources with PySpark.  Each function accepts a
``settings`` dictionary and an active ``SparkSession``.

## `stream_read_cloudfiles`

Create a streaming DataFrame from Auto Loader (``cloudFiles``).
The settings must provide ``readStreamOptions`` and ``readStream_load`` for
the input path along with ``file_schema`` describing the file structure.
A schema derived from ``file_schema`` is passed to ``readStream`` to avoid
schema inference.

## `stream_read_table`

Return a streaming DataFrame that tails an existing Delta table.  Options from
``readStreamOptions`` are applied to the reader before calling ``table`` with
``src_table_name``.

## `read_table`

Batch read a Delta table specified by ``src_table_name``.

## `read_snapshot_windowed`

Return the most recent record for each surrogate key.  The source table is
windowed by ``surrogate_key`` and ordered by ``ingest_time_column`` so only the
latest row for each key is kept.

## `read_latest_ingest`

Load only the rows from the latest ``ingest_time_column`` value in the source
table.
