"""Utilities for integrating Databricks DQX checks.

This module exposes a helper function :func:`apply_dqx_checks` which
runs data quality checks using the `databricks-labs-dqx` library.  The
function is written so that importing this module does not require
``pyspark`` or ``databricks-labs-dqx``.  Dependencies are loaded only
when the helper is executed.
"""

from __future__ import annotations

from typing import Tuple, Any
import uuid
import os
import shutil
from .utility import create_table_if_not_exists
from .dq_checks import (
    min_max,
    is_in,
    is_not_null_or_empty,
    max_length,
    matches_regex_list,
    pattern_match,
    is_null,
    is_nonzero,
    starts_with_prefixes,
)


def apply_dqx_checks(df: Any, settings: dict, spark: Any) -> Tuple[Any, Any]:
    """Apply DQX quality rules to ``df``.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input dataframe to validate.
    settings : dict
        Dictionary with optional key ``dqx_checks`` containing a list of
        check dictionaries in DQX format.
    spark : pyspark.sql.SparkSession
        Active spark session used to create empty dataframes when no
        checks are provided.

    Returns
    -------
    Tuple[DataFrame, DataFrame]
        ``(good_df, bad_df)`` after applying the rules.  When no checks
        are supplied the original dataframe is returned together with an
        empty dataframe having the same schema.
    """

    checks = settings.get("dqx_checks")

    if not checks:
        # Nothing to do - return df unchanged and empty dataframe
        return df, spark.createDataFrame([], df.schema)

    # Import heavy dependencies lazily
    from databricks.labs.dqx.engine import DQEngineCore

    try:
        from databricks.labs.dqx.rule import register_rule
    except Exception:  # pragma: no cover - fallback for older dqx versions
        register_rule = None

    custom_checks = {
        "min_max": min_max,
        "is_in": is_in,
        "is_not_null_or_empty": is_not_null_or_empty,
        "is_null": is_null,
        "max_length": max_length,
        "matches_regex_list": matches_regex_list,
        "pattern_match": pattern_match,
        "is_nonzero": is_nonzero,
        "starts_with_prefixes": starts_with_prefixes,
    }

    if register_rule is not None:
        for func in custom_checks.values():
            register_rule("row")(func)

    class _DummyCurrentUser:
        def me(self):
            return {}

    class _DummyConfig:
        _product_info = ("dqx", "0.0")

    class _DummyWS:
        def __init__(self):
            self.current_user = _DummyCurrentUser()
            self.config = _DummyConfig()

    dq_engine = DQEngineCore(_DummyWS(), spark)
    good_df, bad_df = dq_engine.apply_checks_by_metadata_and_split(
        df, checks, custom_check_functions=custom_checks
    )
    return good_df, bad_df




def create_dqx_bad_records_table(df: Any, settings: dict, spark: Any) -> Any:
    """Validate ``df`` with DQX checks and materialize failures as a table.

    The function mirrors :func:`create_bad_records_table` from
    ``functions.utility``.  Rows failing the checks are written to a table
    named ``<dst_table_name>_dqx_bad_records``.  Any existing table with that
    name is dropped when no failures are found.  If the table exists after
    processing, an exception is raised and the cleaned dataframe is returned.
    """

    dst_table_name = settings.get("dst_table_name")
    if not dst_table_name:
        return df

    df, bad_df = apply_dqx_checks(df, settings, spark)

    dst_bad_table = f"{dst_table_name}_dqx_bad_records"

    checkpoint_location = settings.get("writeStreamOptions", {}).get(
        "checkpointLocation"
    )
    if checkpoint_location is not None:
        base = checkpoint_location.rstrip("/")
        parent = os.path.dirname(base)
        checkpoint_location = f"{parent}/_dqx_checkpoints/"

    if getattr(bad_df, "isStreaming", False):
        if checkpoint_location is None:
            raise ValueError(
                "checkpoint_location must be provided for streaming DataFrames"
            )

        run_id = uuid.uuid4().hex
        location = f"{checkpoint_location.rstrip('/')}/{run_id}/"

        empty_bad_df = spark.createDataFrame([], schema=bad_df.schema)
        (
            empty_bad_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(dst_bad_table)
        )

        (
            bad_df.writeStream.format("delta")
            .option("checkpointLocation", location)
            .trigger(availableNow=True)
            .table(dst_bad_table)
        )
        shutil.rmtree(location, ignore_errors=True)
        n_bad = spark.table(dst_bad_table).count()
    else:
        n_bad = bad_df.count()
        if n_bad > 0:
            create_table_if_not_exists(bad_df, dst_bad_table, spark)
            (
                bad_df.write.mode("overwrite")
                .format("delta")
                .saveAsTable(dst_bad_table)
            )

    if n_bad == 0:
        spark.sql(f"DROP TABLE IF EXISTS {dst_bad_table}")

    if spark.catalog.tableExists(dst_bad_table):
        raise Exception(f"DQX checks failed: {n_bad} failing records")

    return df
