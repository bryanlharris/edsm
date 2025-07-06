#!/usr/bin/env python3
"""Standalone ingest script built from notebook logic.

This script consolidates all helper functions from the ``functions`` package
so that it can run without requiring a wheel or ``--py-files`` on a
serverless cluster.  It expects two arguments:

    --color <color>  (bronze, silver, etc.)
    --table <table>  (table name to ingest)

Settings JSON files are read from S3 using the ``SETTINGS_S3_PREFIX``
environment variable.  The default prefix is ``s3://edsm/settings``.
"""

import argparse
import json
import os
import subprocess
from glob import glob
from pathlib import Path
from urllib.parse import urlparse
import importlib
from typing import Optional

import boto3
import requests
from pyspark.sql.functions import (
    array,
    col,
    concat,
    current_timestamp,
    date_format,
    expr,
    lit,
    regexp_extract,
    regexp_replace,
    row_number,
    sha2,
    struct,
    to_json,
    to_timestamp,
    to_date,
    transform,
    trim,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
#  BEGIN copied functions from functions/utility.py
# ---------------------------------------------------------------------------

JOB_TYPE_MAP = {
    "bronze_standard_streaming": {
        "read_function": "stream_read_cloudfiles",
        "transform_function": "bronze_standard_transform",
        "write_function": "stream_write_table",
    },
    "silver_scd2_streaming": {
        "read_function": "stream_read_table",
        "transform_function": "silver_scd2_transform",
        "write_function": "stream_upsert_table",
        "upsert_function": "microbatch_upsert_scd2_fn",
    },
    "silver_standard_streaming": {
        "read_function": "stream_read_table",
        "transform_function": "silver_standard_transform",
        "write_function": "stream_upsert_table",
        "upsert_function": "microbatch_upsert_fn",
    },
    "silver_scd2_batch": {
        "read_function": "read_table",
        "transform_function": "silver_scd2_transform",
        "write_function": "batch_upsert_scd2",
    },
    "silver_standard_batch": {
        "read_function": "read_table",
        "transform_function": "silver_standard_transform",
        "write_function": "write_upsert_snapshot",
    },
}


def _merge_dicts(base, override):
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


def apply_job_type(settings):
    if str(settings.get("simple_settings", "false")).lower() == "true":
        job_type = settings.get("job_type")
        if not job_type:
            raise KeyError("job_type must be provided when simple_settings is true")
        try:
            defaults = JOB_TYPE_MAP[job_type]
        except KeyError as exc:
            raise KeyError(f"Unknown job_type: {job_type}") from exc

        if job_type == "bronze_standard_streaming":
            dst = settings.get("dst_table_name")
            if not dst:
                raise KeyError("dst_table_name must be provided for bronze_standard_streaming")
            catalog, _, table = dst.split(".", 2)
            base_volume = f"/Volumes/{catalog}/bronze/utility/{table}"
            dynamic = {
                "build_history": "true",
                "readStream_load": f"/Volumes/{catalog}/bronze/landing/",
                "readStreamOptions": {
                    "cloudFiles.inferColumnTypes": "false",
                    "inferSchema": "false",
                    "cloudFiles.schemaLocation": f"{base_volume}/_schema/",
                    "badRecordsPath": f"{base_volume}/_badRecords/",
                    "cloudFiles.rescuedDataColumn": "_rescued_data",
                },
                "writeStreamOptions": {
                    "mergeSchema": "false",
                    "checkpointLocation": f"{base_volume}/_checkpoints/",
                    "delta.columnMapping.mode": "name",
                },
            }
            defaults = _merge_dicts(defaults, dynamic)
        elif job_type.startswith("silver") or job_type.startswith("gold"):
            dynamic = {"build_history": "false", "ingest_time_column": "ingest_time"}
            if "streaming" in job_type:
                dst = settings.get("dst_table_name")
                if not dst:
                    raise KeyError("dst_table_name must be provided for streaming job types")
                catalog, color, table = dst.split(".", 2)
                base_volume = f"/Volumes/{catalog}/{color}/utility/{table}"
                stream_defaults = {
                    "readStreamOptions": {},
                    "writeStreamOptions": {
                        "mergeSchema": "false",
                        "checkpointLocation": f"{base_volume}/_checkpoints/",
                        "delta.columnMapping.mode": "name",
                    },
                }
                dynamic = _merge_dicts(dynamic, stream_defaults)
            defaults = _merge_dicts(defaults, dynamic)
        settings = _merge_dicts(defaults, settings)
    return settings


def get_function(path):
    module_path, func_name = path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)


def create_table_if_not_exists(df, dst_table_name, spark):
    if not spark.catalog.tableExists(dst_table_name):
        empty_df = spark.createDataFrame([], df.schema)
        empty_df.write.format("delta").option("delta.columnMapping.mode", "name").saveAsTable(dst_table_name)
        return True
    return False


def create_schema_if_not_exists(catalog, schema, spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def schema_exists(catalog, schema, spark):
    df = spark.sql(f"SHOW SCHEMAS IN {catalog} LIKE '{schema}'")
    return df.count() > 0


def create_volume_if_not_exists(catalog, schema, volume, spark):
    s3_path = f"s3://edsm/volumes/{catalog}/{schema}/{volume}"
    spark.sql(
        f"CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.{schema}.{volume} LOCATION '{s3_path}'"
    )


def truncate_table_if_exists(table_name, spark):
    if spark.catalog.tableExists(table_name):
        spark.sql(f"TRUNCATE TABLE {table_name}")


def inspect_checkpoint_folder(table_name, settings, spark):
    checkpoint_path = settings.get("writeStreamOptions", {}).get("checkpointLocation")
    checkpoint_path = checkpoint_path.rstrip("/")
    offsets_path = f"{checkpoint_path}/offsets"
    files = glob(f"{offsets_path}/*")
    sorted_files = sorted(files, key=lambda f: int(Path(f).name))
    print(f"{table_name}: batch → bronze version mapping")
    for path in sorted_files:
        batch_id = Path(path).name
        result = subprocess.run(["grep", "reservoirVersion", path], capture_output=True, text=True)
        version = json.loads(result.stdout)["reservoirVersion"]
        print(f"  Silver Batch {batch_id} → Bronze version {version - 1}")


def create_bad_records_table(settings, spark):
    dst_table_name = settings.get("dst_table_name")
    bad_records_path = settings.get("readStreamOptions", {}).get("badRecordsPath")
    if not dst_table_name or not bad_records_path:
        return
    try:
        path = Path(bad_records_path)
        if path.is_dir() and any(path.iterdir()):
            df = spark.read.json(bad_records_path)
            df.write.mode("overwrite").format("delta").saveAsTable(
                f"{dst_table_name}_bad_records"
            )
        else:
            raise FileNotFoundError
    except Exception:
        spark.sql(f"DROP TABLE IF EXISTS {dst_table_name}_bad_records")
    if spark.catalog.tableExists(f"{dst_table_name}_bad_records"):
        raise Exception(f"Bad records table exists: {dst_table_name}_bad_records")

# ---------------------------------------------------------------------------
#  END of utility functions
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
#  BEGIN copied functions from functions/s3_utils.py
# ---------------------------------------------------------------------------

_session: Optional[boto3.session.Session] = None


def _parse_s3_uri(uri: str):
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Invalid S3 URI: {uri}")
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    return bucket, key


def _get_session(dbutils=None) -> boto3.session.Session:
    global _session
    if _session is None:
        access_key = None
        secret_key = None
        if dbutils is not None:
            access_key = dbutils.secrets.get(scope="edsm", key="aws_access_key_id")
            secret_key = dbutils.secrets.get(scope="edsm", key="aws_secret_access_key")
            os.environ["AWS_ACCESS_KEY_ID"] = access_key
            os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key
        else:
            access_key = os.environ.get("AWS_ACCESS_KEY_ID")
            secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        if access_key and secret_key:
            _session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name="us-west-2",
            )
        else:
            _session = boto3.Session()
    return _session


def upload_file(local_path: str, s3_uri: str, dbutils=None):
    bucket, key = _parse_s3_uri(s3_uri)
    client = _get_session(dbutils).client("s3")
    client.upload_file(local_path, bucket, key)


def download_file(s3_uri: str, local_path: str, dbutils=None):
    bucket, key = _parse_s3_uri(s3_uri)
    client = _get_session(dbutils).client("s3")
    client.download_file(bucket, key, local_path)


def read_text(s3_uri: str, dbutils=None) -> str:
    bucket, key = _parse_s3_uri(s3_uri)
    client = _get_session(dbutils).client("s3")
    obj = client.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")

# ---------------------------------------------------------------------------
#  END of s3 utils
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
#  BEGIN copied functions from functions/read.py
# ---------------------------------------------------------------------------


def stream_read_cloudfiles(settings, spark):
    readStreamOptions = settings.get("readStreamOptions")
    readStream_load = settings.get("readStream_load")
    file_schema = settings.get("file_schema")
    schema = StructType.fromJson(settings["file_schema"])
    return (
        spark.readStream
        .format("cloudFiles")
        .options(**readStreamOptions)
        .schema(schema)
        .load(readStream_load)
    )


def stream_read_table(settings, spark):
    src_table_name = settings.get("src_table_name")
    readStreamOptions = settings.get("readStreamOptions")
    return (
        spark.readStream
        .format("delta")
        .options(**readStreamOptions)
        .table(src_table_name)
    )


def read_table(settings, spark):
    return spark.read.table(settings["src_table_name"])


def read_snapshot_windowed(settings, spark):
    surrogate_key = settings["surrogate_key"]
    ingest_time_column = settings["ingest_time_column"]
    window = Window.partitionBy(*surrogate_key).orderBy(col(ingest_time_column).desc())
    return (
        spark.read.table(settings["src_table_name"])
        .withColumn("row_num", row_number().over(window))
        .filter("row_num = 1")
        .drop("row_num")
    )


def read_latest_ingest(settings, spark):
    ingest_time_column = settings["ingest_time_column"]
    df = spark.read.table(settings["src_table_name"])
    max_time = df.agg({ingest_time_column: "max"}).collect()[0][0]
    return df.filter(df[ingest_time_column] == max_time)

# ---------------------------------------------------------------------------
#  END of read functions
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
#  BEGIN copied functions from functions/transform.py
# ---------------------------------------------------------------------------


def bronze_standard_transform(df, settings, spark):
    derived_ingest_time_regex = settings.get("derived_ingest_time_regex", "/(\\d{8})/")
    add_derived = settings.get("add_derived_ingest_time", "false").lower() == "true"
    df = (
        df.transform(clean_column_names)
        .transform(add_source_metadata, settings)
        .withColumn("ingest_time", current_timestamp())
    )
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
    return (
        df.transform(silver_standard_transform, settings, spark)
          .transform(add_scd2_columns, settings, spark)
    )


def add_scd2_columns(df, settings, spark):
    ingest_time_column = settings["ingest_time_column"]
    return (
        df.withColumn("created_on", col(ingest_time_column))
        .withColumn("deleted_on", lit(None).cast("timestamp"))
        .withColumn("current_flag", lit("Yes"))
        .withColumn("valid_from", col(ingest_time_column))
        .withColumn("valid_to", lit("9999-12-31 23:59:59").cast("timestamp"))
    )


def add_source_metadata(df, settings):
    metadata_type = StructType([
        StructField("file_path", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("file_size", LongType(), True),
        StructField("file_block_start", LongType(), True),
        StructField("file_block_length", LongType(), True),
        StructField("file_modification_time", TimestampType(), True),
    ])
    if settings.get("use_metadata", "true").lower() == "true":
        return df.withColumn("source_metadata", expr("_metadata"))
    else:
        return df.withColumn("source_metadata", lit(None).cast(metadata_type))


def make_null_safe(col_expr, dtype):
    if isinstance(dtype, StructType):
        return struct(*[make_null_safe(col_expr[f.name], f.dataType).alias(f.name) for f in dtype.fields])
    elif isinstance(dtype, ArrayType):
        return when(col_expr.isNull(), array().cast(dtype)).otherwise(
            transform(col_expr, lambda x: make_null_safe(x, dtype.elementType))
        )
    elif isinstance(dtype, MapType):
        return col_expr
    else:
        return when(col_expr.isNull(), lit(None).cast(dtype)).otherwise(col_expr)


def normalize_for_hash(df, fields):
    schema = df.schema
    fields_present = [f for f in fields if f in df.columns]
    return df.withColumn(
        "__normalized_struct__",
        struct(*[make_null_safe(col(f), schema[f].dataType).alias(f) for f in fields_present]),
    )


def add_row_hash(df, fields_to_hash, name="row_hash", use_row_hash=False):
    if not use_row_hash:
        return df
    df = normalize_for_hash(df, fields_to_hash)
    return df.withColumn(name, sha2(to_json(col("__normalized_struct__")), 256)).drop("__normalized_struct__")


def clean_column_names(df):
    def clean(name):
        name = name.strip().lower()
        name = re.sub(r"\s+", "_", name)
        name = re.sub(r"[^0-9a-zA-Z_]+", "", name)
        name = re.sub(r"_+", "_", name)
        return name

    def _rec(c, t):
        if isinstance(t, StructType):
            return struct(*[_rec(c[f.name], f.dataType).alias(clean(f.name)) for f in t.fields])
        if isinstance(t, ArrayType):
            return transform(c, lambda x: _rec(x, t.elementType))
        return c

    return df.select(*[_rec(col(f.name), f.dataType).alias(clean(f.name)) for f in df.schema.fields])


def trim_column_values(df, cols=None):
    if cols:
        target_cols = set(c for c in cols if c in df.columns)
    else:
        target_cols = set(c for c, t in df.dtypes if t == "string")
    replacements = {c: trim(col(c)).alias(c) for c in target_cols}
    new_cols = [replacements.get(c, col(c)) for c in df.columns]
    return df.select(new_cols)


def rename_columns(df, column_map=None):
    if not column_map:
        return df
    new_names = [column_map.get(c, c) for c in df.columns]
    return df.toDF(*new_names)


def cast_data_types(df, data_type_map=None):
    if not data_type_map:
        return df
    data_type_map = {c: data_type_map[c] for c in df.columns if c in data_type_map}
    selected_columns = []
    for column_name in df.columns:
        if column_name in data_type_map:
            data_type = data_type_map[column_name]
            if data_type in ["integer", "double", "short", "float"]:
                selected_columns.append(col(column_name).cast(data_type).alias(column_name))
            elif data_type.startswith(("decimal", "numeric")):
                selected_columns.append(regexp_replace(col(column_name), "[$,]", "").cast(data_type).alias(column_name))
            elif data_type == "date":
                selected_columns.append(
                    when(col(column_name).rlike(r"\d{1,2}/\d{1,2}/\d{4}"), to_date(col(column_name), "M/d/yyyy"))
                    .when(col(column_name).rlike(r"\d{1,2}-\d{1,2}-\d{4}"), to_date(col(column_name), "d-M-yyyy"))
                    .when(col(column_name).rlike(r"\d{4}-\d{1,2}-\d{1,2}"), to_date(col(column_name), "yyyy-M-d"))
                    .alias(column_name)
                )
            elif data_type == "timestamp":
                selected_columns.append(
                    when(col(column_name).rlike(r"\d{1,2}/\d{1,2}/\d{4}"), to_date(col(column_name), "M/d/yyyy"))
                    .when(col(column_name).rlike(r"\d{1,2}-\d{1,2}-\d{4}"), to_date(col(column_name), "d-M-yyyy"))
                    .otherwise(to_timestamp(col(column_name)))
                    .alias(column_name)
                )
            else:
                selected_columns.append(col(column_name))
        else:
            selected_columns.append(col(column_name))
    return df.select(selected_columns)

# ---------------------------------------------------------------------------
#  END of transform functions
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
#  BEGIN copied functions from functions/write.py
# ---------------------------------------------------------------------------


def overwrite_table(df, settings, spark):
    df.write.mode("overwrite").saveAsTable(settings["dst_table_name"])


def stream_write_table(df, settings, spark):
    dst_table_name = settings.get("dst_table_name")
    writeStreamOptions = settings.get("writeStreamOptions")
    (
        df.writeStream
        .format("delta")
        .options(**writeStreamOptions)
        .outputMode("append")
        .trigger(availableNow=True)
        .table(dst_table_name)
    )


def stream_upsert_table(df, settings, spark):
    upsert_func = get_function(settings.get("upsert_function"))
    return (
        df.writeStream
        .queryName(settings.get("dst_table_name"))
        .options(**settings.get("writeStreamOptions"))
        .trigger(availableNow=True)
        .foreachBatch(upsert_func(settings, spark))
        .outputMode("update")
        .start()
    )


def _simple_merge(df, settings, spark):
    dst_table_name = settings.get("dst_table_name")
    business_key = settings.get("business_key")
    surrogate_key = settings.get("surrogate_key")
    use_row_hash = str(settings.get("use_row_hash", "false")).lower() == "true"
    row_hash_col = settings.get("row_hash_col", "row_hash")
    merge_condition = " and ".join([f"t.{k} = s.{k}" for k in business_key])
    if use_row_hash:
        change_condition = f"t.{row_hash_col} <> s.{row_hash_col}"
    else:
        change_condition = " or ".join([f"t.{k} <> s.{k}" for k in surrogate_key])
    df.createOrReplaceTempView("updates")
    spark.sql(
        f"""
        MERGE INTO {dst_table_name} t
        USING updates s
        ON {merge_condition}
        WHEN MATCHED AND ({change_condition}) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def upsert_table(df, settings, spark, *, scd2=False, foreach_batch=False, batch_id=None):
    if foreach_batch and batch_id == 0:
        create_table_if_not_exists(df, settings.get("dst_table_name"), spark)
    if scd2:
        _scd2_upsert(df, settings, spark)
    else:
        _simple_merge(df, settings, spark)


def microbatch_upsert_fn(settings, spark):
    def upsert(microBatchDF, batchId):
        upsert_table(
            microBatchDF,
            settings,
            spark,
            scd2=False,
            foreach_batch=True,
            batch_id=batchId,
        )
    return upsert


def _scd2_upsert(df, settings, spark):
    dst_table_name = settings.get("dst_table_name")
    business_key = settings.get("business_key")
    surrogate_key = settings.get("surrogate_key")
    ingest_time_column = settings.get("ingest_time_column")
    use_row_hash = str(settings.get("use_row_hash", "false")).lower() == "true"
    row_hash_col = settings.get("row_hash_col", "row_hash")
    window = Window.partitionBy(*business_key).orderBy(col(ingest_time_column).desc())
    df = df.withColumn("rn", row_number().over(window)).filter("rn = 1").drop("rn")
    merge_condition = " and ".join([f"t.{k} = s.{k}" for k in business_key])
    if use_row_hash:
        change_condition = f"t.{row_hash_col} <> s.{row_hash_col}"
    else:
        change_condition = " or ".join([f"t.{k} <> s.{k}" for k in surrogate_key])
    df.createOrReplaceTempView("updates")
    spark.sql(
        f"""
        MERGE INTO {dst_table_name} t
        USING updates s
        ON {merge_condition} AND t.current_flag='Yes'
        WHEN MATCHED AND ({change_condition}) THEN
            UPDATE SET
                t.deleted_on=s.{ingest_time_column},
                t.current_flag='No',
                t.valid_to=s.{ingest_time_column}
        """
    )
    spark.sql(
        f"""
        INSERT INTO {dst_table_name}
        SELECT
            s.* EXCEPT (created_on, deleted_on, current_flag, valid_from, valid_to),
            s.{ingest_time_column} AS created_on,
            NULL AS deleted_on,
            'Yes' AS current_flag,
            s.{ingest_time_column} AS valid_from,
            CAST('9999-12-31 23:59:59' AS TIMESTAMP) AS valid_to
        FROM updates s
        LEFT JOIN {dst_table_name} t
            ON {merge_condition} AND t.current_flag='Yes'
        WHERE t.current_flag IS NULL
        """
    )


def microbatch_upsert_scd2_fn(settings, spark):
    def upsert(microBatchDF, batchId):
        print(f"Starting batchId: {batchId}, count: {microBatchDF.count()}")
        microBatchDF.show(5, truncate=False)
        upsert_table(
            microBatchDF,
            settings,
            spark,
            scd2=True,
            foreach_batch=True,
            batch_id=batchId,
        )
    return upsert


def batch_upsert_scd2(df, settings, spark):
    upsert_table(df, settings, spark, scd2=True, foreach_batch=False)


def write_upsert_snapshot(df, settings, spark):
    dst_table = settings["dst_table_name"]
    business_key = settings["business_key"]
    ingest_time_col = settings["ingest_time_column"]
    window = Window.partitionBy(*business_key).orderBy(col(ingest_time_col).desc())
    df = df.withColumn("row_num", row_number().over(window)).filter("row_num = 1").drop("row_num")
    df.createOrReplaceTempView("updates")
    merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in business_key])
    spark.sql(
        f"""
    MERGE INTO {dst_table} AS target
    USING updates AS source
    ON {merge_condition}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
    )

# ---------------------------------------------------------------------------
#  END of write functions
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
#  BEGIN copied functions from functions/history.py
# ---------------------------------------------------------------------------


def describe_and_filter_history(full_table_name, spark):
    hist = spark.sql(f"describe history {full_table_name}")
    update_or_merge_version_rows = (
        hist.filter((col("operation") == "STREAMING UPDATE") | (col("operation") == "MERGE"))
        .select("version")
        .distinct()
        .collect()
    )
    version_list = [row["version"] for row in update_or_merge_version_rows]
    return sorted(version_list)


def build_and_merge_file_history(full_table_name, history_schema, spark):
    catalog, schema, table = full_table_name.split(".")
    file_version_table_name = f"{catalog}.{history_schema}.{table}_file_version_history"
    prev_files = set()
    file_version_history_records = []
    version_list = describe_and_filter_history(full_table_name, spark)
    for version in version_list:
        this_version_df = (
            spark.read.format("delta").option("versionAsOf", version).table(full_table_name)
            .select(col("source_metadata.file_path").alias("file_path"))
            .dropDuplicates()
        )
        file_path_list = [row.file_path for row in this_version_df.collect()]
        new_files = set(file_path_list) - prev_files
        prev_files.update(new_files)
        if len(new_files) > 0:
            file_version_history_records.append((version, list(new_files)))
    if len(file_version_history_records) > 0:
        df = spark.createDataFrame(file_version_history_records, "version LONG, file_path ARRAY<STRING>")
        create_table_if_not_exists(df, file_version_table_name, spark)
        df.createOrReplaceTempView("df")
        spark.sql(
            f"""
            merge into {file_version_table_name} as target
            using df as source
            on target.version = source.version
            when matched then update set *
            when not matched then insert *
        """
        )


def transaction_history(full_table_name, history_schema, spark):
    catalog, schema, table = full_table_name.split(".")
    transaction_table_name = f"{catalog}.{history_schema}.{table}_transaction_history"
    df = (
        spark.sql(f"describe history {full_table_name}")
        .withColumn("table_name", lit(full_table_name))
        .selectExpr("table_name", "* except (table_name)")
    )
    create_table_if_not_exists(df, transaction_table_name, spark)
    df.createOrReplaceTempView("df")
    spark.sql(
        f"""
        merge into {transaction_table_name} as target
        using df as source
        on target.version = source.version
        when matched then update set *
        when not matched then insert *
    """
    )

# ---------------------------------------------------------------------------
#  END of history functions
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
#  BEGIN copied functions from functions/rescue.py
# ---------------------------------------------------------------------------


def rescue_silver_table(mode, table_name, spark):
    if mode not in {"timestamp", "versionAsOf"}:
        raise ValueError("mode must be 'timestamp' or 'versionAsOf'")
    settings_path = f"../layer_02_silver/{table_name}.json"
    settings = json.loads(Path(settings_path).read_text())
    settings = apply_job_type(settings)
    transform_function = get_function(settings["transform_function"])
    upsert_function = get_function(settings["upsert_function"])
    upsert = upsert_function(settings, spark)
    spark.sql(f"DROP TABLE IF EXISTS {settings['dst_table_name']}")
    checkpoint_location = settings["writeStreamOptions"]["checkpointLocation"]
    if checkpoint_location.startswith("/Volumes/") and checkpoint_location.endswith("_checkpoints/"):
        subprocess.run(["rm", "-rf", checkpoint_location], check=True)
    else:
        raise ValueError(f"Skipping checkpoint deletion, unsupported path: {checkpoint_location}")
    if mode == "timestamp":
        df = spark.read.table(settings["src_table_name"]).orderBy("derived_ingest_time")
        times = [r[0] for r in df.select("derived_ingest_time").distinct().orderBy("derived_ingest_time").collect()]
        for i, timestamp in enumerate(times):
            batch = df.filter(col("derived_ingest_time") == timestamp)
            batch = transform_function(batch, settings, spark)
            if i == 0:
                create_table_if_not_exists(batch, settings["dst_table_name"], spark)
            upsert(batch, i)
            print(f"{table_name}: Upserted batch {i} for time {timestamp}")
        print(f"{table_name}: Rescue completed in {len(times)} batches")
    else:
        history = spark.sql(f"DESCRIBE HISTORY {settings['src_table_name']}")
        max_version = history.agg({"version": "max"}).first()[0]
        print(f"{table_name}: Max version {max_version}")
        for version in range(0, max_version + 1):
            if version == 0:
                df = spark.read.format("delta").option("versionAsOf", version).table(settings["src_table_name"])
                df = transform_function(df, settings, spark)
                create_table_if_not_exists(df, settings["dst_table_name"], spark)
                print(f"{table_name}: Current version {version}")
                continue
            prev = spark.read.format("delta").option("versionAsOf", version - 1).table(settings["src_table_name"])
            cur = spark.read.format("delta").option("versionAsOf", version).table(settings["src_table_name"])
            df = cur.subtract(prev)
            df = transform_function(df, settings, spark)
            upsert(df, version - 1)
            print(f"{table_name}: Current version {version}")
        settings["readStreamOptions"]["startingVersion"] = max_version
        Path(settings_path).write_text(json.dumps(settings, indent=4))
        print(f"{table_name}: Updated settings with startingVersion {max_version}")


def rescue_silver_table_timestamp(table_name, spark):
    rescue_silver_table("timestamp", table_name, spark)


def rescue_silver_table_versionAsOf(table_name, spark):
    rescue_silver_table("versionAsOf", table_name, spark)


def rescue_gold_table(table_name, spark):
    settings = json.loads(Path(f"../layer_03_gold/{table_name}.json").read_text())
    settings = apply_job_type(settings)
    settings["ingest_time_column"] = "derived_ingest_time"
    history = spark.sql(f"DESCRIBE HISTORY {settings['src_table_name']}")
    max_version = history.agg({"version": "max"}).first()[0]
    print(f"{table_name}: Max version {max_version}")
    spark.sql(f"DROP TABLE IF EXISTS {settings['dst_table_name']}")
    transform_function = get_function(settings["transform_function"])
    write_function = get_function(settings["write_function"])
    for version in range(0, max_version + 1):
        df = spark.read.format("delta").option("versionAsOf", version).table(settings["src_table_name"])
        df = transform_function(df, settings, spark)
        if version == 0:
            create_table_if_not_exists(df, settings["dst_table_name"], spark)
        else:
            write_function(df, settings, spark)
        print(f"Current version: {version}")

# ---------------------------------------------------------------------------
#  END of rescue functions
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
#  BEGIN copied functions from functions/job.py (minimal dependency)
# ---------------------------------------------------------------------------


def save_job_configuration(dbutils, path=None):
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    if path is None:
        try:
            catalog = dbutils.widgets.get("catalog")
        except Exception:
            catalog = "edsm"
        path = f"/Volumes/{catalog}/bronze/utility/jobs"
    job_id = ctx.jobId().getOrElse(None)
    if job_id is None:
        raise RuntimeError("Not running inside a job")
    host = os.environ.get("DATABRICKS_HOST") or ctx.apiUrl().get()
    token = os.environ.get("DATABRICKS_TOKEN") or ctx.apiToken().get()
    url = f"{host}/api/2.1/jobs/get"
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params={"job_id": job_id})
    resp.raise_for_status()
    job_config = resp.json()
    job_name = job_config.get("settings", {}).get("name", f"job-{job_id}")
    dst = Path(path) / f"{job_name}.json"
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(json.dumps(job_config, indent=4))

# ---------------------------------------------------------------------------
#  END of job helper
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
#  Ingest main logic from notebook
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Ingest a table using settings from S3")
    parser.add_argument("--color", required=True)
    parser.add_argument("--table", required=True)
    args = parser.parse_args()

    color = args.color
    table = args.table
    prefix = os.environ.get("SETTINGS_S3_PREFIX", "s3://edsm/settings")
    settings_uri = f"{prefix}/{color}/{table}.json"

    raw = read_text(settings_uri)
    settings = json.loads(raw)
    settings = apply_job_type(settings)
    dst_table_name = settings["dst_table_name"]

    settings_message = f"\n\nSettings URI: {settings_uri}\n\n"
    settings_message += json.dumps(settings, indent=4)
    print(settings_message)

    if "pipeline_function" in settings:
        pipeline_function = globals()[settings["pipeline_function"].split(".")[-1]]
        pipeline_function(settings, spark)
    elif all(k in settings for k in ["read_function", "transform_function", "write_function"]):
        read_function = globals()[settings["read_function"].split(".")[-1]]
        transform_function = globals()[settings["transform_function"].split(".")[-1]]
        write_function = globals()[settings["write_function"].split(".")[-1]]
        df = read_function(settings, spark)
        df = transform_function(df, settings, spark)
        write_function(df, settings, spark)
    else:
        raise Exception("Could not find any ingest function name in settings.")

    if color == "bronze":
        create_bad_records_table(settings, spark)


if __name__ == "__main__":
    main()
