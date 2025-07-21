# Bronze layer

Bronze tables ingest the raw EDSM export files using Databricks Auto Loader.
Each table is described by a JSON file under `layer_01_bronze`.  These files
usually set `simple_settings` to `true` and `job_type` to
`bronze_standard_streaming`.

## Standard streaming job

When the job type is `bronze_standard_streaming` the settings are expanded at
runtime.  The following functions are applied:

- `functions.read.stream_read_cloudfiles` reads new files from the landing path.
- `functions.transform.bronze_standard_transform` attaches metadata and cleans
the data.
- `functions.write.stream_write_table` writes the result to the destination
  Delta table.

Additional options such as `readStream_load`, `readStreamOptions` and
`writeStreamOptions` are derived from `dst_table_name`.  Checkpoints and schema
information are stored under `utility/<table>` while input files are loaded from
`landing/`.

## Transform details

`bronze_standard_transform` adds an `ingest_time` column and copies ingestion
metadata from `_metadata` into `source_metadata`.  If
`add_derived_ingest_time` is enabled a `derived_ingest_time` field is extracted
from the file path using `derived_ingest_time_regex`.

When Auto Loader is configured with a `badRecordsPath`, those files can be
converted into a `<dst_table_name>_bad_records` table using
`create_bad_records_table`.

## History tables

Bronze jobs default to `build_history: true`.  When `history_schema` is
provided, a `<table>_file_ingestion_history` table is maintained alongside the
bronze table.  This table records every file that contributes to a given Delta
version along with the full transaction metadata from `DESCRIBE HISTORY`.  The
history table uses the same column types as the original command so maps and
structs are preserved.  If a Delta version cannot be read due to missing files,
that version is skipped.

The history table allows downstream processes to track which source files
produced each version of the bronze table.
