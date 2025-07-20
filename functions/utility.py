import os
import json
import importlib
import subprocess
import html
from glob import glob
from pathlib import Path
from pyspark.sql.types import StructType
from collections import defaultdict, deque

from .config import JOB_TYPE_MAP, S3_ROOT_LANDING, S3_ROOT_UTILITY, OBJECT_OWNER


def print_settings(job_settings, settings, color, table):
    """Display formatted job and table settings with copy buttons."""

    try:
        from dbruntime.display import displayHTML  # type: ignore
    except Exception:  # pragma: no cover - fallback when not on Databricks
        try:
            from IPython.display import display as displayHTML  # type: ignore
        except Exception:  # pragma: no cover - running outside notebooks
            displayHTML = print  # type: ignore

    def _copy_block(text: str, elem_id: str) -> str:
        escaped = html.escape(text)
        return (
            f"<button onclick=\"copyJson('{elem_id}')\">Copy JSON to Clipboard</button>"
            f"<textarea id=\"{elem_id}\" style=\"display:none;\">{escaped}</textarea>"
            f"<pre>{escaped}</pre>"
        )

    job_html = _copy_block(json.dumps(job_settings, indent=4), "job-json")
    table_html = _copy_block(json.dumps(settings, indent=4), "table-json")

    displayHTML(
        f"""
<h3>Dictionary from {color}_settings.json:</h3>
{job_html}
<h3>Derived contents of {table}.json:</h3>
{table_html}
<script>
function copyJson(id) {{
    var textArea = document.getElementById(id);
    textArea.style.display = 'block';
    textArea.select();
    document.execCommand('copy');
    textArea.style.display = 'none';
}}
</script>
"""
    )




def _merge_dicts(base, override):
    """Recursively merge two dictionaries.

    ``override`` values take precedence over ``base``. Nested dictionaries are
    merged rather than replaced.
    """

    result = base.copy()
    for key, value in override.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, dict)
        ):
            result[key] = _merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


def apply_job_type(settings):
    """Expand ``job_type`` into explicit function names.

    When ``settings`` includes ``simple_settings`` set to ``true`` (case
    insensitive), the corresponding functions from ``JOB_TYPE_MAP`` are merged
    into the settings dictionary. Existing keys in ``settings`` take precedence.
    For the ``bronze_standard_streaming`` job type, additional defaults derived
    from ``dst_table_name`` are merged in, and nested dictionaries are combined.
    """

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
            catalog, schema, table = dst.split(".", 2)
            base_volume = f"/Volumes/{catalog}/{schema}/utility/{table}"
            dynamic = {
                "build_history": "true",
                "readStream_load": f"/Volumes/{catalog}/{schema}/landing/",
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


def sort_by_dependency(dependencies):
    """Return a topologically sorted list from a dependency map.

    Parameters
    ----------
    dependencies : dict[str, list[str]]
        Mapping of table name to list of tables it depends on.

    Returns
    -------
    list[str]
        Tables ordered so that all dependencies appear before a table.

    Raises
    ------
    Exception
        If a dependency cycle is detected.
    """

    graph = defaultdict(list)
    in_degree = {t: 0 for t in dependencies}

    for table, deps in dependencies.items():
        for dep in deps:
            if dep in dependencies:
                graph[dep].append(table)
                in_degree[table] += 1

    queue = deque([t for t, d in in_degree.items() if d == 0])
    ordered = []

    while queue:
        node = queue.popleft()
        ordered.append(node)
        for neighbor in graph[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(ordered) != len(dependencies):
        raise Exception("Dependency cycle detected")

    return ordered


def get_function(path):
    """Return a callable from a dotted module path.

    ``path`` should be a fully qualified object name such as
    ``"functions.read.stream_read_cloudfiles"``.  The string is split
    once from the right with ``path.rsplit(".", 1)`` so any dots before
    the last one become part of the module path and the final segment is
    the attribute name.  A ``ValueError`` is raised if no ``'.'`` is present.
    ``ModuleNotFoundError`` or ``AttributeError`` propagate when the
    module or attribute cannot be imported.
    """

    module_path, func_name = path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)


def _extract_owner(df):
    """Return the owner value from a DESCRIBE EXTENDED dataframe."""

    try:
        rows = df.collect()
    except Exception:
        return None

    for row in rows:
        for attr in [
            "database_description_item",
            "col_name",
            "info_name",
        ]:
            key = getattr(row, attr, None)
            if key and str(key).lower() == "owner":
                for val_attr in [
                    "database_description_value",
                    "data_type",
                    "info_value",
                ]:
                    value = getattr(row, val_attr, None)
                    if value is not None:
                        return str(value)
        if len(row) >= 2 and str(row[0]).lower() == "owner":
            return str(row[1])

    return None


def _ensure_admin_owner(obj_type: str, name: str, spark) -> None:
    """Ensure the Databricks object is owned by ``config.OBJECT_OWNER``."""

    describe_map = {
        "table": f"DESCRIBE TABLE EXTENDED {name}",
        "schema": f"DESCRIBE SCHEMA EXTENDED {name}",
        "volume": f"DESCRIBE VOLUME EXTENDED {name}",
    }
    alter_map = {
        "table": f"ALTER TABLE {name} OWNER TO `{OBJECT_OWNER}`",
        "schema": f"ALTER SCHEMA {name} OWNER TO `{OBJECT_OWNER}`",
        "volume": f"ALTER VOLUME {name} OWNER TO `{OBJECT_OWNER}`",
    }

    try:
        df = spark.sql(describe_map[obj_type])
    except Exception:
        return

    owner = _extract_owner(df)
    if owner and owner != OBJECT_OWNER:
        try:
            spark.sql(alter_map[obj_type])
            print(f"\tINFO: Owner changed to {OBJECT_OWNER} for {obj_type} {name}.")
        except Exception:
            print(f"\tWARNING: Failed to change owner for {obj_type} {name}.")
    elif not owner:
        try:
            spark.sql(alter_map[obj_type])
            print(f"\tINFO: Owner set to {OBJECT_OWNER} for {obj_type} {name}.")
        except Exception:
            print(f"\tWARNING: Failed to set owner for {obj_type} {name}.")


def create_table_if_not_exists(df, dst_table_name, spark):
    """Create a table from a dataframe if it doesn't exist.

    Returns
    -------
    bool
        ``True`` if the table was created, ``False`` otherwise.
    """

    if not spark.catalog.tableExists(dst_table_name):
        empty_df = spark.createDataFrame([], df.schema)
        (
            empty_df.write
            .format("delta")
            .option("delta.columnMapping.mode", "name")
            .saveAsTable(dst_table_name)
        )
        print(f"\tINFO: Table did not exist and was created: {dst_table_name}.")
        created = True
    else:
        created = False

    _ensure_admin_owner("table", dst_table_name, spark)

    return created


def create_schema_if_not_exists(catalog, schema, spark):
    """Create the schema if it is missing and print a message."""

    if not schema_exists(catalog, schema, spark):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
        print(f"\tINFO: Schema did not exist and was created: {catalog}.{schema}.")

    _ensure_admin_owner("schema", f"{catalog}.{schema}", spark)

def schema_exists(catalog, schema, spark):
    """Return True if the schema exists in the given catalog"""
    df = spark.sql(f"SHOW SCHEMAS IN {catalog} LIKE '{schema}'")
    return df.count() > 0


def catalog_exists(catalog, spark):
    """Return True if the catalog exists"""
    df = spark.sql(f"SHOW CATALOGS LIKE '{catalog}'")
    return df.count() > 0


def volume_exists(catalog, schema, volume, spark):
    """Return True if the external volume exists."""

    df = spark.sql(f"SHOW VOLUMES IN {catalog}.{schema} LIKE '{volume}'")
    return df.count() > 0


def create_volume_if_not_exists(catalog, schema, volume, spark):
    """Create an external volume pointing to the expected S3 path."""

    if not volume_exists(catalog, schema, volume, spark):
        root = S3_ROOT_LANDING if volume == "landing" else S3_ROOT_UTILITY
        s3_path = f"{root}{catalog}/{schema}/{volume}"
        spark.sql(
            f"CREATE EXTERNAL VOLUME IF NOT EXISTS {catalog}.{schema}.{volume} LOCATION '{s3_path}'"
        )
        print(
            f"\tINFO: Volume did not exist and was created: /Volumes/{catalog}/{schema}/{volume}."
        )

    _ensure_admin_owner("volume", f"{catalog}.{schema}.{volume}", spark)


def truncate_table_if_exists(table_name, spark):
    """Truncate ``table_name`` if it already exists."""

    if spark.catalog.tableExists(table_name):
        spark.sql(f"TRUNCATE TABLE {table_name}")

def inspect_checkpoint_folder(table_name, settings, spark):
    """Print batch to version mapping from a Delta checkpoint folder."""

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
    """Create a delta table from the JSON files located in ``badRecordsPath``.

    If the path does not exist, any existing table ``<dst_table_name>_bad_records``
    is dropped.  If the table exists after this function runs, an exception is
    raised to signal that bad records were found.
    """

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


def parse_si(value):
    """Return ``value`` converted to a float using SI notation."""

    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        raise TypeError("value must be a number or string")
    s = value.strip().lower()
    scale = {"k": 1e3, "m": 1e6, "g": 1e9, "t": 1e12}
    if s and s[-1] in scale:
        return float(s[:-1]) * scale[s[-1]]
    return float(s)
