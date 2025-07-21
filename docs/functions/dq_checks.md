# `functions.dq_checks`

Helper functions implementing custom row-level checks for Databricks
`databricks-labs-dqx`. Each function returns a Spark Column that describes
any validation failure when used with DQX rules.

## `min_max`

Return a column that fails when values fall outside the provided minimum and maximum.

## `is_in`

Return a column that fails when values are not contained in the provided list.

## `is_not_null_or_empty`

Return a column that fails when the input is null or an empty string. The
optional `trim_strings` argument strips whitespace before the check.

## `max_length`

Return a column that fails when the string length is greater than the supplied maximum.

## `matches_regex_list`

Return a column that fails when the value does not match any regular expression in the list.

## `pattern_match`

Wrapper around `matches_regex_list` for a single pattern.

## `is_nonzero`

Return a column that fails when the value is equal to zero.

## `is_null`

Return a column that fails when the value is not null.

## `starts_with_prefixes`

Return a column that fails when the value does not start with any of the provided prefixes.
