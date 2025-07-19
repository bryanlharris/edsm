from .utility import create_table_if_not_exists, truncate_table_if_exists
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    sha2,
    to_json,
    struct,
)

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
    """Create or update a file ingestion history table combining lineage and transaction details.

    Each new row includes an ``ingest_time`` column with the timestamp when the
    record was added to the history table.
    """

    catalog, schema, table = full_table_name.split(".")
    ingestion_table_name = f"{catalog}.{history_schema}.{table}_file_ingestion_history"
    ingestion_exists = spark.catalog.tableExists(ingestion_table_name)
    if ingestion_exists:
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
        df = df.withColumn(
            "row_hash",
            sha2(
                to_json(
                    struct(
                        "file_path",
                        *[c for c in hist_df.columns if c != "version"],
                    )
                ),
                256,
            ),
        )
        table_created = create_table_if_not_exists(df, ingestion_table_name, spark)

        table_cols = []
        if ingestion_exists:
            table_cols = spark.table(ingestion_table_name).columns
            if "row_hash" not in table_cols:
                spark.sql(f"ALTER TABLE {ingestion_table_name} ADD COLUMNS (row_hash STRING)")
                existing_df = spark.table(ingestion_table_name)
                existing_df = existing_df.withColumn(
                    "row_hash",
                    sha2(
                        to_json(
                            struct(
                                "file_path",
                                *[c for c in existing_df.columns if c not in ("version", "row_hash")],
                            )
                        ),
                        256,
                    ),
                )
                existing_df.createOrReplaceTempView("existing_df")
                spark.sql(
                    f"""
                    MERGE INTO {ingestion_table_name} as target
                    USING existing_df as source
                    ON target.file_path = source.file_path AND target.version = source.version
                    WHEN MATCHED THEN UPDATE SET row_hash = source.row_hash
                    """
                )
                table_cols.append("row_hash")

        df.createOrReplaceTempView("df")

        merge_condition = (
            "target.row_hash = source.row_hash"
            if (table_created or "row_hash" in table_cols)
            else "target.file_path = source.file_path and target.version = source.version"
        )

        spark.sql(
            f"""
            merge into {ingestion_table_name} as target
            using df as source
            on {merge_condition}
            when matched then update set *
            when not matched then insert *
        """
        )

def transaction_history(full_table_name, history_schema, spark):
    """Backward compatible wrapper for ``build_and_merge_file_history``."""

    build_and_merge_file_history(full_table_name, history_schema, spark)
