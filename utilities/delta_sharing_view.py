from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DeltaSharingTest") \
    .config("spark.jars.packages", "io.delta:delta-sharing-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sql("""
    CREATE OR REPLACE TEMP VIEW test_view
    USING deltaSharing
    OPTIONS (
      path "file:///home/sqltest/source/edsm/config.share#edsm.silver.test_partitioned"
    )
""")

result_df = spark.sql("SELECT name, power FROM test_view")
result_df.show()

