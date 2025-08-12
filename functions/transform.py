"""Transformation utilities used throughout the project.

These helpers standardize schemas, cleanse data and provide sampling
mechanisms for Spark DataFrames.  See ``docs/functions/transform.md`` for
additional documentation.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
    ArrayType,
    MapType,
)
from pyspark.sql.functions import (
    concat,
    regexp_extract,
    date_format,
    current_timestamp,
    when,
    col,
    to_timestamp,
    to_date,
    regexp_replace,
    sha2,
    lit,
    trim,
    struct,
    to_json,
    expr,
    transform,
    array,
    rand,
    conv,
    substring,
    hash,
    pmod,
)
import re

from .utility import parse_si


def bronze_standard_transform(df, settings, spark):
    """Apply standard bronze layer transformations."""

    derived_ingest_time_regex = settings.get("derived_ingest_time_regex", "/(\\d{8})/")
    add_derived = settings.get("add_derived_ingest_time", "false").lower() == "true"
    df = (
        df.transform(clean_column_names)
        .transform(add_source_metadata, settings)
    )

    df = df.withColumn("ingest_time", current_timestamp())

    if add_derived:
        df = df.withColumn(
            "derived_ingest_time",
            to_timestamp(
                concat(
                    regexp_extract(col("source_metadata.file_path"), derived_ingest_time_regex, 1),
                    lit(" "),
                    date_format(current_timestamp(), "HH:mm:ss"),
                ),
                "yyyyMMdd HH:mm:ss",
            ),
        )

    return df

def silver_standard_transform(df, settings, spark):
    """Apply common silver layer cleaning logic."""

    # Settings
    surrogate_key = settings.get("surrogate_key", [])
    column_map = settings.get("column_map", None)
    data_type_map = settings.get("data_type_map", None)
    use_row_hash = str(settings.get("use_row_hash", "false")).lower() == "true" and len(surrogate_key) > 0
    row_hash_col = settings.get("row_hash_col", "row_hash")

    return (
        df.transform(rename_columns, column_map)
        .transform(cast_data_types, data_type_map)
        .withColumn("file_path", col("source_metadata").getField("file_path"))
        .withColumn("file_modification_time", col("source_metadata").getField("file_modification_time"))
        .transform(add_row_hash, surrogate_key, row_hash_col, use_row_hash)
    )


def silver_scd2_transform(df, settings, spark):
    """Prepare a DataFrame for SCD2 upserts in the silver layer."""

    return (
        df.transform(silver_standard_transform, settings, spark)
          .transform(add_scd2_columns, settings, spark)
    )


def add_scd2_columns(df, settings, spark):
    """Attach standard SCD2 tracking columns."""

    ingest_time_column = settings["ingest_time_column"]
    return (
        df.withColumn("created_on", col(ingest_time_column))
        .withColumn("deleted_on", lit(None).cast("timestamp"))
        .withColumn("current_flag", lit("Yes"))
        .withColumn("valid_from", col(ingest_time_column))
        .withColumn("valid_to", lit("9999-12-31 23:59:59").cast("timestamp"))
    )


def add_source_metadata(df, settings):
    """Attach ingestion metadata to a DataFrame."""
    metadata_type = StructType([
        StructField("file_path", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("file_size", LongType(), True),
        StructField("file_block_start", LongType(), True),
        StructField("file_block_length", LongType(), True),
        StructField("file_modification_time", TimestampType(), True)
    ])

    if settings.get("use_metadata", "true").lower() == "true":
        return df.withColumn("source_metadata", expr("_metadata"))
    else:
        return df.withColumn("source_metadata", lit(None).cast(metadata_type))


def make_null_safe(col_expr, dtype):
    """Ensure nested fields exist so hashing comparisons are stable."""
    if isinstance(dtype, StructType):
        return struct(*[
            make_null_safe(col_expr[f.name], f.dataType).alias(f.name)
            for f in dtype.fields
        ])
    elif isinstance(dtype, ArrayType):
        return when(col_expr.isNull(), array().cast(dtype)) \
            .otherwise(transform(col_expr, lambda x: make_null_safe(x, dtype.elementType)))
    elif isinstance(dtype, MapType):
        return col_expr
    else:
        return when(col_expr.isNull(), lit(None).cast(dtype)).otherwise(col_expr)


def normalize_for_hash(df, fields):
    """Normalize fields to stable structures for hashing."""

    schema = df.schema
    fields_present = [f for f in fields if f in df.columns]
    return df.withColumn(
        "__normalized_struct__",
        struct(*[
            make_null_safe(col(f), schema[f].dataType).alias(f)
            for f in fields_present
        ])
    )

def add_row_hash(df, fields_to_hash, name="row_hash", use_row_hash=False):
    """Compute a hash over selected fields and store it in ``name``."""

    if not use_row_hash:
        return df

    df = normalize_for_hash(df, fields_to_hash)

    return df.withColumn(name, sha2(to_json(col("__normalized_struct__")), 256)).drop("__normalized_struct__")


def add_row_hash_mod(df, column_name, modulus):
    """Add a column with ``column_name`` converted from hex to int and modded.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame containing ``column_name``.
    column_name : str
        Name of the column containing a hexadecimal hash value.
    modulus : int
        Value to mod the integer representation by.
    """
    return df.withColumn(
        "row_hash_mod",
        (
            conv(substring(col(column_name), 1, 16), 16, 10)
            .cast("decimal(38,0)") % modulus
        ).cast("long")
    )


def clean_column_names(df):
    """Normalize column names by removing spaces and invalid characters."""
    def clean(name):
        name = name.strip().lower()
        name = re.sub(r"\s+", "_", name)
        name = re.sub(r"[^0-9a-zA-Z_]+", "", name)
        name = re.sub(r"_+", "_", name)
        return name

    def _rec(c, t):
        if isinstance(t, StructType):
            return struct(*[
                _rec(c[f.name], f.dataType).alias(clean(f.name))
                for f in t.fields
            ])
        if isinstance(t, ArrayType):
            return transform(c, lambda x: _rec(x, t.elementType))
        return c

    return df.select(*[
        _rec(col(f.name), f.dataType).alias(clean(f.name))
        for f in df.schema.fields
    ])


def trim_column_values(df, cols=None):
    """Strip whitespace from string columns."""

    if cols:
        target_cols = set(c for c in cols if c in df.columns)
    else:
        target_cols = set(c for c, t in df.dtypes if t == "string")

    replacements = {c: trim(col(c)).alias(c) for c in target_cols}
    new_cols = [replacements.get(c, col(c)) for c in df.columns]

    return df.select(new_cols)


def rename_columns(df, column_map=None):
    """Rename DataFrame columns using ``column_map``."""

    if not column_map:
        return df
    new_names = [column_map.get(c, c) for c in df.columns]
    return df.toDF(*new_names)


def cast_data_types(df, data_type_map=None):
    """Cast columns to the provided data types."""

    if not data_type_map:
        return df

    data_type_map   = {c: data_type_map[c] for c in df.columns if c in data_type_map}

    type_casters = {
        "integer": lambda c, dt: col(c).cast(dt).alias(c),
        "double": lambda c, dt: col(c).cast(dt).alias(c),
        "short": lambda c, dt: col(c).cast(dt).alias(c),
        "float": lambda c, dt: col(c).cast(dt).alias(c),
        "decimal": lambda c, dt: regexp_replace(col(c), "[$,]", "").cast(dt).alias(c),
        "numeric": lambda c, dt: regexp_replace(col(c), "[$,]", "").cast(dt).alias(c),
        "date": lambda c, dt: when(col(c).rlike(r"\d{1,2}/\d{1,2}/\d{4}"), to_date(col(c), "M/d/yyyy"))
                            .when(col(c).rlike(r"\d{1,2}-\d{1,2}-\d{4}"), to_date(col(c), "d-M-yyyy"))
                            .when(col(c).rlike(r"\d{4}-\d{1,2}-\d{1,2}"), to_date(col(c), "yyyy-M-d"))
                            .alias(c),
        "timestamp": lambda c, dt: when(col(c).rlike(r"\d{1,2}/\d{1,2}/\d{4}"), to_date(col(c), "M/d/yyyy"))
                               .when(col(c).rlike(r"\d{1,2}-\d{1,2}-\d{4}"), to_date(col(c), "d-M-yyyy"))
                               .otherwise(to_timestamp(col(c))).alias(c),
    }

    selected_columns = []
    for column_name in df.columns:
        if column_name in data_type_map:
            data_type = data_type_map[column_name]
            caster = None
            for key, func in type_casters.items():
                if data_type == key or data_type.startswith(key):
                    caster = func
                    break
            if caster:
                selected_columns.append(caster(column_name, data_type))
            else:
                selected_columns.append(col(column_name))
        else:
            selected_columns.append(col(column_name))

    return df.select(selected_columns)


def sample_table(df, settings, spark):
    """Return a sample of ``df`` based on ``sample_type`` and ``sample_fraction``.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    settings : dict
        Configuration dictionary that may include ``sample_type`` (``random`` or
        ``deterministic``), ``sample_fraction``, and ``hash_modulus`` when using
        deterministic sampling.
    spark : SparkSession
        Unused but included for API consistency.
    """

    sample_type = str(settings.get("sample_type", "random")).lower()
    fraction = float(settings.get("sample_fraction", 0.01))

    if "_rescued_data" in df.columns:
        df = df.drop("_rescued_data")

    if sample_type == "deterministic":
        modulus = int(settings.get("hash_modulus", 1000000))
        threshold = int(fraction * modulus)
        row_hash_col = settings.get("row_hash_col", "row_hash")

        if row_hash_col not in df.columns:
            df = df.transform(add_row_hash, df.columns, row_hash_col, True)

        df = df.transform(add_row_hash_mod, row_hash_col, modulus)
        return df.where(col("row_hash_mod") < threshold).drop("row_hash_mod")

    if sample_type == "simple":
        id_col = settings["sample_id_col"]
        sample_size = parse_si(settings["sample_size"])
        src_table_name = settings["src_table_name"]
        if not spark.catalog.tableExists(src_table_name):
            return df

        total = (
            spark.sql(f"SELECT count(*) AS total FROM {src_table_name}")
            .collect()[0][0]
        )
        modulus = max(int(total / sample_size), 1)
        df = df.where(col(id_col).isNotNull())
        return df.where(pmod(hash(col(id_col)), modulus) == 0)

    return df.where(rand() < fraction)






