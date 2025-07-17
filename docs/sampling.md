# Sampling Data

The `sample_table` function returns a subset of a DataFrame. The
behaviour is controlled by a few settings passed in the `settings` dictionary.

## Sample type

The `sample_type` setting controls whether sampling is random or deterministic.
If omitted, random sampling is used.

* `random` – rows are selected based on the output of `rand()`.
* `deterministic` – rows are selected using a stable hash of each row. This
  ensures the same subset is returned each time when the input data is
  unchanged.

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
