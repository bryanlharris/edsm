from functions.utility_functions import create_table_if_not_exists
from pyspark.sql.functions import col

def describe_and_filter_history(spark, full_table_name):
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

def build_and_merge_file_history(spark, full_table_name):
    file_version_table_name = f"{full_table_name}_file_version_history"
    prev_files = set()
    file_version_history_records = []
    version_list = describe_and_filter_history(spark, full_table_name)

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
            file_version_history_records.append((f"{full_table_name}_{version}", list(new_files)))

    if len(file_version_history_records) > 0:

        # Create df
        df = spark.createDataFrame(file_version_history_records, "primary_key STRING, file_path ARRAY<STRING>")

        # Sanity check
        create_table_if_not_exists(spark, df, file_version_table_name)

        # Write
        df.createOrReplaceTempView("df")
        spark.sql(f"""
            merge into {file_version_table_name} as target
            using df as source
            on target.primary_key = source.primary_key
            when matched then update set *
            when not matched then insert *
        """)
