"""Custom transform functions.

Add project specific transformation functions here. Each function should
accept ``(df, settings, spark)`` and return a DataFrame.

Example
-------

.. code-block:: python

    def my_transform(df, settings, spark):
        return df

Use the fully qualified path when referencing a custom transform in a
settings file:

.. code-block:: json

    {
        "transform_function": "functions.custom.my_transform"
    }
"""
