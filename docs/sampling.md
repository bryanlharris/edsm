# Sampling Data

The `sample_table` function returns a subset of a DataFrame. The
behaviour is controlled by a few settings passed in the `settings` dictionary.

## Sample type

The `sample_type` setting controls how rows are selected.
If omitted, random sampling is used.

* `random` – rows are selected based on the output of `rand()`.
* `deterministic` – rows are selected using a stable hash of each row. This
  ensures the same subset is returned each time when the input data is
  unchanged. Deterministic sampling accepts either `sample_fraction` values
  between 0 and 1 or an absolute `sample_size` expressed using SI notation
  such as `1k`.
* `simple` – rows are selected by hashing a specific ID column and applying
  ``pmod(hash(id), N) = 0``. When ``sample_fraction`` is supplied the modulus
  ``N`` is derived from the inverse of the fraction. With ``sample_size`` it is
  calculated as ``count(*) / sample_size`` using the row count of the table
  referenced by ``src_table_name``. The ``settings`` dictionary must include
  ``sample_id_col`` naming the column to hash and either ``sample_fraction`` or
  ``sample_size`` to specify how many rows to keep. These options are mutually
  exclusive and ``sample_size`` supports SI notation such as ``1k`` or ``5m``.
  Only rows where the ID is not null are considered. When ``sample_size`` is
  used and the table named by ``src_table_name`` does not exist ``sample_table``
  returns the input unchanged so ``initialize_empty_tables`` can run without
  errors.

## Sample fraction

`sample_fraction` specifies the fraction of rows to return. Values must be
between 0 and 1. When using `deterministic` sampling the fraction is applied
against the `hash_modulus` value. In `random` mode the default value `0.01`
(1%) is used when `sample_fraction` is not provided. Provide either
`sample_fraction` **or** `sample_size` when using deterministic or simple sampling.

## Sample size

`sample_size` provides an absolute threshold for deterministic or simple sampling and may
use SI notation such as `1k` or `5m`. It is mutually exclusive with
`sample_fraction` and may be supplied instead of a fraction when using
deterministic or simple sampling.

## Hash modulus

When `sample_type` is `deterministic`, the `hash_modulus` setting determines the
size of the modulus space used to compute the threshold. The value supports SI
notation such as `1M` and defaults to `1000000` when not supplied.

If a column named by `row_hash_col` (default `row_hash`) already exists, it will
be used for deterministic sampling; otherwise the column is created using all
columns as the hash input.

## Example

### Using `sample_fraction`

```python
settings = {
    "sample_type": "deterministic",
    "sample_fraction": 0.05,
    "hash_modulus": "1M",
}

sampled_df = sample_table(df, settings, spark)
```

This configuration returns roughly 5% of `df` using a stable hash based on a
modulus of one million.

### Using `sample_size`

```python
settings = {
    "sample_type": "deterministic",
    "sample_size": "10k",  # ten thousand rows
    "hash_modulus": "1M",  # modulus of one million
}

sampled_df = sample_table(df, settings, spark)
```

This approach keeps approximately 10,000 rows using deterministic sampling.

### Simple sampling with `sample_fraction`

```json
{
    "sample_type": "simple",
    "sample_id_col": "id",
    "sample_fraction": 0.1
}
```

This configuration keeps roughly 10% of the rows based on the hashed `id` column.

## Persisting samples

When using `sample_table` to materialize a sampled table, configure the job to
overwrite the destination rather than merge updates. Add the following setting
so old rows do not accumulate across runs:

```json
"write_function": "functions.write.overwrite_table"
```
