from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DeltaSharingTest") \
    .config("spark.jars.packages", "io.delta:delta-sharing-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.format("deltaSharing") \
    .option("responseFormat", "delta") \
    .load("file:///home/sqltest/source/edsm/config.share#edsm.silver.test_partitioned")

df.select("name", "power").show()

