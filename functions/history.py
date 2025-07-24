from .utility import (
    create_table_if_not_exists,
    truncate_table_if_exists,
    schema_exists,
)
from .transform import add_row_hash
from pyspark.sql.functions import col, lit, current_timestamp

def describe_and_filter_history(full_table_name, spark):
    """Return ordered versions produced by tracked Delta operations."""

    hist = spark.sql(f"describe history {full_table_name}")
    version_rows = (
        hist.filter(
            (col("operation") == "STREAMING UPDATE")
            | (col("operation") == "STREAMING MERGE")
            | (col("operation") == "MERGE")
            | (col("operation") == "WRITE")
            | (col("operation") == "UPDATE")
            | (col("operation") == "DELETE")
            | (col("operation") == "RESTORE")
        )
        .select("version")
        .distinct()
        .collect()
    )
    version_list = [row["version"] for row in version_rows]
    version_list = sorted(version_list)
    return version_list

def build_and_merge_file_history(full_table_name, history_schema, spark, dst_table_name=None):
    """Create or update a file ingestion history table combining lineage and transaction details.

    Parameters
    ----------
    full_table_name : str
        Fully qualified source table name used to read ``DESCRIBE HISTORY``.
    history_schema : str
        Schema where the history table will be stored.
    spark : SparkSession
    dst_table_name : str, optional
        Fully qualified destination table name. When ``None`` the name is
        derived from ``full_table_name`` and ``history_schema`` with
        ``"_file_ingestion_history"`` appended.

    Each new row includes an ``ingest_time`` column with the timestamp when the
    record was added to the history table.
    """

    catalog, schema, table = full_table_name.split(".")
    if dst_table_name:
        ingestion_table_name = dst_table_name
    else:
        ingestion_table_name = f"{catalog}.{history_schema}.{table}_file_ingestion_history"
    if spark.catalog.tableExists(ingestion_table_name):
        last_version = (
            spark.table(ingestion_table_name)
            .agg({"version": "max"})
            .collect()[0][0]
        )
        if last_version is None:
            last_version = -1
    else:
        last_version = -1

    hist_df = spark.sql(f"describe history {full_table_name}")
    current_max_version = hist_df.agg({"version": "max"}).collect()[0][0]
    if current_max_version is None:
        current_max_version = -1

    if last_version > current_max_version:
        truncate_table_if_exists(ingestion_table_name, spark)
        last_version = -1

    version_list = [
        v for v in describe_and_filter_history(full_table_name, spark) if v > last_version
    ]

    if last_version >= 0:
        prev_files = {
            row.file_path
            for row in (
                spark.read
                .format("delta")
                .option("versionAsOf", last_version)
                .table(full_table_name)
                .select(col("source_metadata.file_path").alias("file_path"))
                .dropDuplicates()
                .collect()
            )
        }
    else:
        prev_files = set()

    records_df = None

    for version in version_list:
        try:
            this_version_df = (
                spark.read
                .format("delta")
                .option("versionAsOf", version)
                .table(full_table_name)
                .select(col("source_metadata.file_path").alias("file_path"))
                .dropDuplicates()
            )
            file_paths = [row.file_path for row in this_version_df.collect()]
        except Exception as e:  # pragma: no cover - Spark error paths are tested manually
            msg = str(e)
            if "DELTA_FILE_NOT_FOUND_DETAILED" in msg or "DBR_FILE_NOT_EXIST" in msg:
                print(f"Skipping version {version}: {msg}")
                continue
            raise

        new_files = set(file_paths) - prev_files
        prev_files.update(new_files)
        if new_files:
            file_df = spark.createDataFrame([(fp,) for fp in new_files], "file_path STRING")
            file_df = file_df.withColumn("table_name", lit(full_table_name))
            hist_row_df = hist_df.filter(col("version") == version)
            new_df = file_df.crossJoin(hist_row_df).withColumn(
                "ingest_time", current_timestamp()
            )
            records_df = new_df if records_df is None else records_df.unionByName(new_df)

    if records_df is not None:
        df = records_df.select(
            "file_path",
            "table_name",
            "ingest_time",
            *hist_df.columns,
        )
        # hash only the columns relevant for deduplication
        hash_cols = ["file_path", "table_name", "timestamp", "ingest_time"]
        df = df.transform(add_row_hash, hash_cols, "row_hash", True)
        create_table_if_not_exists(df, ingestion_table_name, spark)
        df.createOrReplaceTempView("df")
        spark.sql(
            f"""
            merge into {ingestion_table_name} as target
            using df as source
            on target.row_hash = source.row_hash
            when matched then update set *
            when not matched then insert *
        """
        )


def history_pipeline(settings, spark):
    build_history = str(settings.get("build_history", "false")).lower() == "true"
    if not build_history:
        return

    full_table = settings.get("full_table_name", settings.get("dst_table_name"))
    history_schema = settings.get("history_schema")
    if not history_schema:
        print("Skipping history build: no history_schema provided")
        return

    catalog = full_table.split(".")[0]
    dst_history_table = settings.get("dst_table_name")
    if not dst_history_table:
        base = full_table.split(".")[-1]
        dst_history_table = f"{catalog}.{history_schema}.{base}_file_ingestion_history"
        print(
            f"\tWARNING: dst_table_name not provided; defaulting to {dst_history_table}"
        )
    if schema_exists(catalog, history_schema, spark):
        build_and_merge_file_history(full_table, history_schema, spark, dst_history_table)
    else:
        print(f"Skipping history build: schema {catalog}.{history_schema} not found")


