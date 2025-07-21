# `functions.transform`

## Main transforms

### `bronze_standard_transform`
Apply default bronze logic such as cleaning column names, attaching file metadata and adding an `ingest_time` column.

Parameters
- **df**: input DataFrame
- **settings**: controls options like `derived_ingest_time_regex` and `add_derived_ingest_time`
- **spark**: Spark session (unused)

### `silver_standard_transform`
Normalize silver data by renaming columns, casting data types and copying selected metadata. When `use_row_hash` is true a hash is generated for the `surrogate_key` fields.

Parameters
- **df**: input DataFrame
- **settings**: may include `surrogate_key`, `column_map`, `data_type_map` and `use_row_hash`
- **spark**: Spark session (unused)

### `silver_scd2_transform`
Prepare rows for slowly changing dimension type 2 (SCD2) processing by running `silver_standard_transform` and attaching SCD2 tracking columns.

Parameters
- **df**: input DataFrame
- **settings**: requires `ingest_time_column`
- **spark**: Spark session (unused)

## Common transforms

### `sample_table`
Return a subset of rows based on the sampling configuration.

Parameters
- **df**: input DataFrame
- **settings**: may contain `sample_type`, `sample_fraction`, `hash_modulus`, `sample_id_col` and `sample_size`
- **spark**: Spark session used for the `simple` sampling mode

### `rename_columns`
Rename DataFrame columns using the provided mapping.

Parameters
- **df**: input DataFrame
- **column_map**: dictionary mapping original names to new names

### `cast_data_types`
Cast columns to the configured data types.

Parameters
- **df**: input DataFrame
- **data_type_map**: dictionary mapping column names to Spark SQL types

### `clean_column_names`
Remove whitespace and invalid characters from column names recursively.

Parameters
- **df**: input DataFrame

### `add_row_hash`
Compute a SHA‑256 hash over selected fields and store it in the provided column name when `use_row_hash` is enabled.

Parameters
- **df**: input DataFrame
- **fields_to_hash**: list of columns to hash
- **name**: destination column, default `row_hash`
- **use_row_hash**: boolean flag controlling whether the hash is added

### `trim_column_values`
Strip whitespace from all string columns or the provided subset.

Parameters
- **df**: input DataFrame
- **cols**: optional list of column names

## Helper transforms

### `add_source_metadata`
Attach a `source_metadata` struct column derived from `_metadata` with file path, size and modification time. When `use_metadata` is false the column is filled with nulls.

Parameters
- **df**: input DataFrame
- **settings**: may disable metadata capture with `use_metadata`

### `add_row_hash_mod`
Add a numeric modulus of a row hash for deterministic sampling.

Parameters
- **df**: input DataFrame
- **column_name**: column containing the hexadecimal hash
- **modulus**: integer modulus value

### `normalize_for_hash`
Normalize fields so that complex structures hash consistently.

Parameters
- **df**: input DataFrame
- **fields**: list of columns to normalize

### `make_null_safe`
Ensure nested struct or array fields exist when hashing.

Parameters
- **col_expr**: column expression being normalized
- **dtype**: expected Spark SQL type

### `add_scd2_columns`
Attach standard SCD2 tracking columns used by `silver_scd2_transform`.

Parameters
- **df**: input DataFrame
- **settings**: must include `ingest_time_column`
- **spark**: Spark session (unused)


