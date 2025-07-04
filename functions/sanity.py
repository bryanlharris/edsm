import json
from pathlib import Path
from pyspark.sql.types import StructType
from functions.utility import create_table_if_not_exists, get_function, apply_job_type


def _discover_settings_files(project_root):
    """Return dictionaries of settings files for each layer."""
    project_root = Path(project_root)
    bronze_files = {
        f.stem: str(f)
        for f in project_root.glob("layer_*_bronze/*.json")
    }
    silver_files = {
        f.stem: str(f)
        for f in project_root.glob("layer_*_silver/*.json")
    }
    gold_files = {
        f.stem: str(f)
        for f in project_root.glob("layer_*_gold/*.json")
    }

    return bronze_files, silver_files, gold_files

def validate_settings(project_root, dbutils):
    """Ensure all settings files contain required keys before processing."""

    ## Check that all json settings files have the minimum required keys AKA functions before proceeding
    bronze_inputs = dbutils.jobs.taskValues.get(taskKey="job_settings", key="bronze")
    silver_inputs = dbutils.jobs.taskValues.get(taskKey="job_settings", key="silver")
    gold_inputs = dbutils.jobs.taskValues.get(taskKey="job_settings", key="gold")

    bronze_files, silver_files, gold_files = _discover_settings_files(project_root)

    all_tables = set(list(bronze_files.keys()) + list(silver_files.keys()) + list(gold_files.keys()))

    layers=["bronze","silver","gold"]
    required_keys={
        "bronze":["read_function","transform_function","write_function","dst_table_name","file_schema"],
        "silver":["read_function","transform_function","write_function","src_table_name","dst_table_name"],
        "gold":["read_function","transform_function","write_function","src_table_name","dst_table_name"]
    }


    write_key_requirements = {
        "functions.write.stream_upsert_table": [
            "business_key",
            "surrogate_key",
            "upsert_function",
        ],
        "functions.write.batch_upsert_scd2": [
            "business_key",
            "surrogate_key",
            "upsert_function",
        ],
        "functions.write.write_upsert_snapshot": ["business_key"],
    }

    errs = []

    # Check for required functions
    for layer, files in [("bronze", bronze_files), ("silver", silver_files), ("gold", gold_files)]:
        for tbl, path in files.items():
            settings=json.loads(open(path).read())
            settings = apply_job_type(settings)
            for k in required_keys[layer]:
                if k not in settings:
                    errs.append(f"{path} missing {k}")
            write_fn = settings.get("write_function")
            if write_fn in write_key_requirements:
                for req_key in write_key_requirements[write_fn]:
                    if req_key not in settings:
                        errs.append(f"{path} missing {req_key} for write_function {write_fn}")

    if errs:
        raise RuntimeError("Sanity check failed: "+", ".join(errs))
    else:
        print("Sanity check: Validate settings check passed.")


def initialize_empty_tables(project_root, spark):
    """Create empty Delta tables based on settings definitions."""

    errs = []
    bronze_files, silver_files, gold_files = _discover_settings_files(project_root)

    all_tables = set(list(bronze_files.keys()) + list(silver_files.keys()) + list(gold_files.keys()))

    layers=["bronze","silver","gold"]

    ## For each table and each layer, cascade transforms and create table
    for tbl in sorted(all_tables):
        df=None
        skip_table=False
        for layer in layers:
            if layer=="bronze" and tbl not in bronze_files:
                break
            if layer=="silver" and tbl not in silver_files:
                break
            if layer=="gold" and tbl not in gold_files:
                break
            if layer=="bronze":
                path=bronze_files[tbl]
            elif layer=="silver":
                path=silver_files[tbl]
            elif layer=="gold":
                path=gold_files[tbl]
            settings=json.loads(open(path).read())
            settings = apply_job_type(settings)
            if layer=="bronze":
                settings["use_metadata"] = "false"
                if "file_schema" not in settings:
                    errs.append(f"{path} missing file_schema, cannot create table")
                    skip_table=True
                    break
                schema=StructType.fromJson(settings["file_schema"])
                df=spark.createDataFrame([], schema)
            try:
                transform_function = get_function(settings["transform_function"])
            except Exception:
                errs.append(f"{path} missing transform_function for {tbl}, cannot create table")
                skip_table=True
                break
            df=transform_function(df, settings, spark)
            dst=settings["dst_table_name"]
            if create_table_if_not_exists(df, dst, spark):
                print(f"\tINFO: Table did not exist and was created: {dst}.")
        if skip_table:
            continue

    if errs:
        raise RuntimeError("Sanity check failed: "+", ".join(errs))
    else:
        print("Sanity check: Initialize empty tables check passed.")








