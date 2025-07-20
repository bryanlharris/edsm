# `functions.config`

This module stores constants used across the ingestion notebooks and
utility functions.

## `PROJECT_ROOT`

The absolute path to the project root on the local filesystem.  Other
modules reference this when loading files relative to the repository.

## `JOB_TYPE_MAP`

A dictionary mapping short ``job_type`` names to the functions that make
up an ingest pipeline.  `apply_job_type` merges these defaults into a
settings dictionary when ``simple_settings`` is enabled.

The provided job types are:

- **bronze_standard_streaming** – reads raw files from CloudFiles,
  transforms them with ``bronze_standard_transform`` and writes with
  ``stream_write_table``.
- **silver_scd2_streaming** – reads a source table, applies
  ``silver_scd2_transform`` and performs microbatch SCD2 upserts.
- **silver_upsert_streaming** – similar to the above but uses a standard
  transform and simple microbatch upserts.
- **silver_standard_streaming** – reads from a table, runs the standard
  transform and writes the stream without upserts.
- **silver_scd2_batch** – batch reads and upserts using SCD2 semantics.
- **silver_standard_batch** – batch reads and writes a snapshot table.
- **gold_standard_batch** – batch reads silver data and writes a snapshot
  table.

## S3 roots

``S3_ROOT_LANDING`` and ``S3_ROOT_UTILITY`` define the base S3 locations
for external landing and utility volumes.
Both values should include a trailing ``/``.  When omitted, it will be
appended by ``sanity.validate_s3_roots`` but updating ``config.py`` is
recommended.
