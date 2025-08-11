# Custom transforms

Add your own transformation functions in `functions/custom.py`. Each
function should accept three arguments:

```
(df, settings, spark)
```

Reference the function by its dotted path in a settings file so it can be
loaded with `functions.utility.get_function` when the job runs.

```python
# functions/custom.py
def my_transform(df, settings, spark):
    # custom logic here
    return df
```

Settings snippet:

```json
{
  "transform_function": "functions.custom.my_transform"
}
```

When the job executes, ``get_function`` imports ``functions.custom`` and
calls ``my_transform`` with the DataFrame, settings, and spark session.

Transforms are linked through ``src_table_name`` and ``dst_table_name`` fields
in the settings files. Dependency order is resolved from this lineage, so
settings across layers do not need matching filenames—only the correct table
references.
