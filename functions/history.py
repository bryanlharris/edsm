from .utility import create_table_if_not_exists, truncate_table_if_exists
from pyspark.sql.functions import col, lit

def describe_and_filter_history(full_table_name, spark):
    """Return ordered versions that correspond to streaming updates or merges."""

    hist = spark.sql(f"describe history {full_table_name}")
    update_or_merge_version_rows = (
        hist.filter((col("operation") == "STREAMING UPDATE") | (col("operation") == "MERGE"))
        .select("version")
        .distinct()
        .collect()
    )
    version_list = [row["version"] for row in update_or_merge_version_rows]
    version_list = sorted(version_list)
    return version_list

def build_and_merge_file_history(full_table_name, history_schema, spark):
    """Create or update a file ingestion history table combining lineage and transaction details."""

    catalog, schema, table = full_table_name.split(".")
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

    if version_list:
        hist_dict = {
            row['version']: row.asDict(recursive=True)
            for row in hist_df.collect()
        }
    else:
        hist_dict = {}

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

    records = []

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
            hist_row = hist_dict.get(version)
            for fp in new_files:
                records.append({"file_path": fp, "table_name": full_table_name, **hist_row})

    if records:
        df = spark.createDataFrame(records).select(
            "file_path",
            "table_name",
            *hist_df.columns,
        )
        create_table_if_not_exists(df, ingestion_table_name, spark)
        df.createOrReplaceTempView("df")
        spark.sql(
            f"""
            merge into {ingestion_table_name} as target
            using df as source
            on target.file_path = source.file_path and target.version = source.version
            when matched then update set *
            when not matched then insert *
        """
        )

def transaction_history(full_table_name, history_schema, spark):
    """Backward compatible wrapper for ``build_and_merge_file_history``."""

    build_and_merge_file_history(full_table_name, history_schema, spark)
