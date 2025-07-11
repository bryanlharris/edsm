import json
from .utility import create_table_if_not_exists, truncate_table_if_exists
from pyspark.sql.functions import col, lit, expr

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
    """Create a file history table tracking new files across versions."""

    catalog, schema, table = full_table_name.split(".")
    file_version_table_name = f"{catalog}.{history_schema}.{table}_file_version_history"
    if spark.catalog.tableExists(file_version_table_name):
        last_version = (
            spark.table(file_version_table_name)
            .agg({"version": "max"})
            .collect()[0][0]
        )
    else:
        last_version = -1

    current_max_version = (
        spark.sql(f"describe history {full_table_name}")
        .agg({"version": "max"})
        .collect()[0][0]
    )

    if last_version > current_max_version:
        truncate_table_if_exists(file_version_table_name, spark)
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

    file_version_history_records = []

    for version in version_list:
        this_version_df = (
            spark.read
            .format("delta")
            .option("versionAsOf", version)
            .table(full_table_name)
            .select(col("source_metadata.file_path").alias("file_path"))
            .dropDuplicates()
        )
        file_path_list = this_version_df.collect()
        file_path_list = [row.file_path for row in file_path_list]
        new_files = set(file_path_list) - prev_files
        prev_files.update(new_files)
        if len(new_files) > 0:
            file_version_history_records.append((version, list(new_files)))

    if len(file_version_history_records) > 0:
        df = spark.createDataFrame(file_version_history_records, "version LONG, file_path ARRAY<STRING>")
        create_table_if_not_exists(df, file_version_table_name, spark)
        df.createOrReplaceTempView("df")
        spark.sql(f"""
            merge into {file_version_table_name} as target
            using df as source
            on target.version = source.version
            when matched then update set *
            when not matched then insert *
        """)

def transaction_history(full_table_name, history_schema, spark):
    """Record the Delta transaction history for ``full_table_name``."""

    catalog, schema, table = full_table_name.split(".")
    transaction_table_name = f"{catalog}.{history_schema}.{table}_transaction_history"
    df = (
        spark.sql(f"describe history {full_table_name}")
        .withColumn("table_name", lit(full_table_name))
        .selectExpr("table_name", "* except (table_name)")
    )
    create_table_if_not_exists(df, transaction_table_name, spark)
    df.createOrReplaceTempView("df")
    spark.sql(f"""
        merge into {transaction_table_name} as target
        using df as source
        on target.version = source.version
        when matched then update set *
        when not matched then insert *
    """)










