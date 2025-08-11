import json
import os
from pyspark.sql.types import StructType
from functions.utility import (
    create_table_if_not_exists,
    get_function,
    apply_job_type,
    create_schema_if_not_exists,
    create_volume_if_not_exists,
    catalog_exists,
    schema_exists,
)
from functions.dependencies import sort_by_dependency, build_dependency_graph
from functions import config
PROJECT_ROOT = config.PROJECT_ROOT


def _discover_settings_files():
    """Return dictionaries of settings files for each layer including samples."""
    project_root = PROJECT_ROOT
    bronze_files = {
        f.stem: str(f)
        for f in project_root.glob("layer_*_bronze/*.json")
    }
    silver_files = {
        f.stem: str(f)
        for f in project_root.glob("layer_*_silver/*.json")
    }
    silver_sample_files = {
        f.stem: str(f)
        for f in project_root.glob("layer_*_silver_samples/*.json")
    }
    gold_files = {
        f.stem: str(f)
        for f in project_root.glob("layer_*_gold/*.json")
    }

    return bronze_files, silver_files, silver_sample_files, gold_files


def validate_s3_roots():
    """Ensure S3 root constants end with a trailing slash."""

    updated = False

    if not config.S3_ROOT_LANDING.endswith("/"):
        config.S3_ROOT_LANDING += "/"
        print(
            "\tWARNING: Added trailing '/' to S3_ROOT_LANDING; update config.py to include it."
        )
        updated = True

    if not config.S3_ROOT_UTILITY.endswith("/"):
        config.S3_ROOT_UTILITY += "/"
        print(
            "\tWARNING: Added trailing '/' to S3_ROOT_UTILITY; update config.py to include it."
        )
        updated = True

    return updated

def validate_settings(dbutils):
    """Ensure all settings files contain required keys before processing."""

    ## Check that all json settings files have the minimum required keys AKA functions before proceeding
    bronze_inputs = dbutils.jobs.taskValues.get(taskKey="job_settings", key="bronze")
    silver_inputs = dbutils.jobs.taskValues.get(taskKey="job_settings", key="silver")
    silver_parallel = dbutils.jobs.taskValues.get(taskKey="job_settings", key="silver_parallel") or []
    silver_sequential = dbutils.jobs.taskValues.get(taskKey="job_settings", key="silver_sequential") or []
    gold_inputs = dbutils.jobs.taskValues.get(taskKey="job_settings", key="gold")

    bronze_files, silver_files, silver_sample_files, gold_files = _discover_settings_files()

    all_tables = set(
        list(bronze_files.keys())
        + list(silver_files.keys())
        + list(silver_sample_files.keys())
        + list(gold_files.keys())
    )

    layers = ["bronze", "silver", "silver_samples", "gold"]
    required_keys={
        "bronze":["read_function","transform_function","write_function","dst_table_name","file_schema"],
        "silver":["read_function","transform_function","write_function","src_table_name","dst_table_name"],
        "gold":["read_function","transform_function","write_function","src_table_name","dst_table_name"],
        "silver_samples":["read_function","transform_function","write_function","src_table_name","dst_table_name"],
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
    for layer, files in [
        ("bronze", bronze_files),
        ("silver", silver_files),
        ("silver_samples", silver_sample_files),
        ("gold", gold_files),
    ]:
        for tbl, path in files.items():
            settings = json.loads(open(path).read())
            settings = apply_job_type(settings)
            # Skip validation when a pipeline function is used
            if "pipeline_function" in settings:
                continue
            for k in required_keys[layer]:
                if k not in settings:
                    errs.append(f"{path} missing {k}")
            write_fn = settings.get("write_function")
            if write_fn in write_key_requirements:
                for req_key in write_key_requirements[write_fn]:
                    if req_key not in settings:
                        errs.append(
                            f"{path} missing {req_key} for write_function {write_fn}"
                        )

    # Validate silver table dependencies
    silver_defined = {name.lower() for name in silver_files.keys()}
    for item in silver_sequential:
        tbl = item.get("table")
        for dep in item.get("requires", []):
            if dep.lower() not in silver_defined:
                errs.append(
                    f"Silver table {tbl} requires missing silver table {dep}"
                )
    try:
        sort_by_dependency(silver_sequential)
    except ValueError as exc:
        errs.append(str(exc))

    if errs:
        raise RuntimeError("Sanity check failed: "+", ".join(errs))
    else:
        print("Sanity check: Validate settings check passed.")

    # Validate S3 root configuration after settings are confirmed
    validate_s3_roots()



def initialize_empty_tables(spark):
    """Create empty Delta tables based on settings definitions."""

    errs = []
    bronze_files, silver_files, silver_sample_files, gold_files = _discover_settings_files()

    # Load all settings keyed by normalized destination table name
    settings_map = {}
    for files in [bronze_files, silver_files, silver_sample_files, gold_files]:
        for path in files.values():
            settings = json.loads(open(path).read())
            settings = apply_job_type(settings)
            settings["_path"] = path  # retain for error messages
            dst = settings.get("dst_table_name")
            if dst:
                settings_map[dst.lower()] = settings

    # Determine processing order based on src -> dst lineage
    ordered = sort_by_dependency(build_dependency_graph(settings_map.values()))

    for item in ordered:
        settings = settings_map[item["table"]]
        path = settings["_path"]
        src = settings.get("src_table_name")

        if src:
            try:
                df = spark.table(src).limit(0)
            except Exception:
                errs.append(f"{path} missing source table {src}, cannot create table")
                continue
        else:
            settings["use_metadata"] = "false"
            if "file_schema" not in settings:
                errs.append(f"{path} missing file_schema, cannot create table")
                continue
            schema = StructType.fromJson(settings["file_schema"])
            df = spark.createDataFrame([], schema)

        try:
            transform_function = get_function(settings["transform_function"])
        except Exception:
            errs.append(
                f"{path} missing transform_function for {settings.get('dst_table_name')}, cannot create table"
            )
            continue

        df = transform_function(df, settings, spark)
        dst = settings["dst_table_name"]
        create_table_if_not_exists(df, dst, spark)

    if errs:
        raise RuntimeError("Sanity check failed: " + ", ".join(errs))
    else:
        print("Sanity check: Initialize empty tables check passed.")


def initialize_schemas_and_volumes(spark):
    """Create schemas and external volumes based on settings definitions."""

    bronze_files, silver_files, silver_sample_files, gold_files = _discover_settings_files()

    errs = []

    schemas = {
        "bronze": set(),
        "silver": set(),
        "silver_samples": set(),
        "gold": set(),
        "history": set(),
    }
    file_map = {
        "bronze": bronze_files,
        "silver": silver_files,
        "silver_samples": silver_sample_files,
        "gold": gold_files,
    }
    catalogs = set()

    for color, files in file_map.items():
        for path in files.values():
            settings = json.loads(open(path).read())
            settings = apply_job_type(settings)
            dst = settings.get("dst_table_name")
            if not dst:
                continue
            catalog, schema, _ = dst.split(".", 2)
            catalogs.add(catalog)
            schemas[color].add((catalog, schema))
            if str(settings.get("build_history", "false")).lower() == "true":
                history_schema = settings.get("history_schema")
                if history_schema:
                    schemas["history"].add((catalog, history_schema))

    for color in ["bronze", "silver", "silver_samples", "gold"]:
        if len(schemas[color]) > 1:
            errs.append(
                f"Multiple schemas discovered for {color}: {sorted(schemas[color])}"
            )
    if len(schemas["history"]) > 1:
        errs.append(
            f"Multiple history schemas discovered: {sorted(schemas['history'])}"
        )

    if len(catalogs) > 1:
        errs.append(f"Multiple catalogs discovered: {sorted(catalogs)}")
    elif catalogs:
        catalog = next(iter(catalogs))
        if not catalog_exists(catalog, spark):
            errs.append(f"Catalog does not exist: {catalog}")

    if errs:
        raise RuntimeError("Sanity check failed: " + ", ".join(errs))

    volume_map = {
        "bronze": ["landing", "utility"],
        "silver": ["utility"],
        "silver_samples": ["utility"],
        "gold": ["utility"],
    }

    missing_history = False

    for color in ["bronze", "silver", "silver_samples", "gold", "history"]:
        for catalog, schema in sorted(schemas[color]):
            if color == "history" and not schema_exists(catalog, schema, spark):
                print(f"\tWARNING: History schema does not exist: {catalog}.{schema}")
                missing_history = True
            create_schema_if_not_exists(catalog, schema, spark)
            spark.sql(f"GRANT USAGE ON SCHEMA {catalog}.{schema} TO `account users`")
            for volume in volume_map.get(color, []):
                create_volume_if_not_exists(catalog, schema, volume, spark)

    if missing_history:
        print("Sanity check: Initialize schemas and volumes check completed with warnings.")
    else:
        print("Sanity check: Initialize schemas and volumes check passed.")


