# Sampling Data

The `sample_table` function returns a subset of a DataFrame. The
behaviour is controlled by a few settings passed in the `settings` dictionary.

## Sample type

The `sample_type` setting controls how rows are selected.
If omitted, random sampling is used.

* `random` – rows are selected based on the output of `rand()`.
* `deterministic` – rows are selected using a stable hash of each row. This
  ensures the same subset is returned each time when the input data is
  unchanged.
* `simple` – rows are selected by hashing a specific ID column and applying
  ``pmod(hash(id), N) = 0``. ``N`` is calculated as ``approx_count_distinct(*) /
  sample_size`` using the table referenced by ``src_table_name``. The ``settings``
  dictionary must include ``sample_id_col`` naming the column to hash and
  ``sample_size`` specifying how many rows to keep. The ``sample_size`` value
  supports SI notation such as ``1k`` or ``5m``. Only rows where the ID is not
  null are considered. ``sample_fraction`` is ignored for this mode. When the
  table named by ``src_table_name`` does not exist ``sample_table`` returns the
  input unchanged so ``initialize_empty_tables`` can run without errors.

## Sample fraction

`sample_fraction` specifies the fraction of rows to return. When using
`deterministic` sampling the fraction is applied against the `hash_modulus`
value. If this option is not provided the default value `0.01` (1%) is used.

## Hash modulus

When `sample_type` is `deterministic`, the `hash_modulus` setting determines the
size of the modulus space used to compute the threshold. It defaults to
`1000000` when not supplied.

If a column named by `row_hash_col` (default `row_hash`) already exists, it will
be used for deterministic sampling; otherwise the column is created using all
columns as the hash input.

## Example

```python
settings = {
    "sample_type": "deterministic",
    "sample_fraction": 0.05,
    "hash_modulus": 2000000,
}

sampled_df = sample_table(df, settings, spark)
```

This configuration returns roughly 5% of `df` using a stable hash based on a
modulus of 2,000,000.

### Using `sample_size`

```python
settings = {
    "sample_type": "simple",
    "sample_id_col": "id",
    "sample_size": "1k",
    "src_table_name": "my_database.my_table",
}

sampled_df = sample_table(df, settings, spark)
```

This approach keeps approximately 1,000 rows by hashing the `id` column.

## Persisting samples

When using `sample_table` to materialize a sampled table, configure the job to
overwrite the destination rather than merge updates. Add the following setting
so old rows do not accumulate across runs:

```json
"write_function": "functions.write.overwrite_table"
```
