# `functions.sanity`

Helpers for validating configuration files and preparing the environment
before ingestion runs.

## `_discover_settings_files`

Return dictionaries mapping table names to the JSON settings files for
the bronze, silver, silver sample and gold layers.

## `validate_settings`

Ensure each settings file contains the required keys for its layer.
Jobs that define a `pipeline_function` skip these checks so custom
pipelines such as the history job can omit the standard
`read_function`, `transform_function` and `write_function` keys.
Extra requirements are enforced for certain write functions. Raises an
exception when any file fails validation. When settings are valid it also
calls `validate_s3_roots` to warn about missing trailing slashes in the
S3 root constants.

## `initialize_empty_tables`

Create empty Delta tables based on the configured transforms. Each table
is built layer by layer starting from an empty DataFrame and written with
`create_table_if_not_exists`. The read/transform/write chain cascades
through bronze, silver, silver samples and gold layers.

## `initialize_schemas_and_volumes`

Create catalogs, schemas and external volumes referenced by the settings.
History schemas are added when enabled. When a history schema does not exist a
warning is emitted before the schema is created. An error is raised if multiple
catalogs or schemas are discovered. Silver sample schemas are processed like
the other layers so empty tables can be initialized.

## `validate_s3_roots`

Ensure ``S3_ROOT_LANDING`` and ``S3_ROOT_UTILITY`` include a trailing
``/``. Missing slashes are appended and a warning is printed so the
values can be updated in ``functions.config``.
