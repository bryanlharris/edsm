import sys
from pathlib import Path
from pyspark.sql import SparkSession

# Add project root to PYTHONPATH to import functions
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from functions.utility import _extract_owner


def get_owner(obj_type: str, name: str, spark) -> str:
    describe_map = {
        "table": f"DESCRIBE TABLE EXTENDED {name}",
        "schema": f"DESCRIBE SCHEMA EXTENDED {name}",
        "volume": f"DESCRIBE VOLUME EXTENDED {name}",
    }
    try:
        df = spark.sql(describe_map[obj_type])
    except Exception as exc:
        print(f"Failed to describe {obj_type} {name}: {exc}")
        return None
    return _extract_owner(df)


def main():
    spark = SparkSession.builder.appName("owner-check").getOrCreate()
    objects = [
        ("volume", "edsm.bronze.landing"),
        ("schema", "edsm.bronze"),
        ("table", "edsm.bronze.powerplay"),
    ]
    for obj_type, name in objects:
        owner = get_owner(obj_type, name, spark)
        if owner:
            print(f"{obj_type} {name} owner: {owner}")
        else:
            print(f"{obj_type} {name} owner not found")
    spark.stop()


if __name__ == "__main__":
    main()
