#!/usr/bin/env python
"""Query a Delta Sharing table with PySpark.

Usage:
  python delta_sharing_example.py [profile.share] [delta_sharing.table.path]

The profile file is the JSON returned from ``DESCRIBE RECIPIENT``. The table
path should match an entry inside that file.
"""

import sys
from pyspark.sql import SparkSession


def main(profile: str = "profile.share", table: str = "delta_sharing.catalog.schema.table") -> None:
    spark = (
        SparkSession.builder
        .appName("DeltaSharingClient")
        .config("spark.sql.extensions", "io.delta.sharing.spark.DeltaSharingSparkSessionExtension")
        .config("spark.sql.catalog.delta_sharing", "io.delta.sharing.spark.DeltaSharingCatalog")
        .config("spark.sql.catalog.delta_sharing.shareCredentialsFile", profile)
        .getOrCreate()
    )

    df = spark.read.table(table)
    df.show()
    spark.stop()


if __name__ == "__main__":
    profile_file = sys.argv[1] if len(sys.argv) > 1 else "profile.share"
    table_path = sys.argv[2] if len(sys.argv) > 2 else "delta_sharing.catalog.schema.table"
    main(profile_file, table_path)

