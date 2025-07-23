# Delta Sharing

Delta Sharing lets you publish tables from a Databricks catalog and query them outside of Databricks.
This guide shows the SQL commands to create a share and a PySpark example for reading the data.

## Share a catalog

Run these SQL commands on Databricks to share a table.

```sql
CREATE SHARE my_share;
ALTER SHARE my_share ADD TABLE catalog.schema.table;
CREATE RECIPIENT my_recipient USING IDENTITY 'sharing';
GRANT SELECT ON SHARE my_share TO RECIPIENT my_recipient;
DESCRIBE RECIPIENT my_recipient;
```

`DESCRIBE RECIPIENT` prints a URL to a `.share` file containing your credentials.
Download the JSON and save it as `profile.share`.

### Create the share in the Databricks UI

If you prefer the website interface, you can create the share without running
any SQL. In the Databricks workspace navigate to **Data → Delta Sharing → Shares**
and click **Create Share**. Choose a name, add the tables you want to expose and
create a recipient. Once the recipient is created you can download the
corresponding `.share` credential file from the UI.

## Query with PySpark

Install the Delta Sharing client.

```bash
pip install delta-sharing
```

Use the credential file with Spark to read the table.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("DeltaSharingClient")
    .config("spark.sql.extensions", "io.delta.sharing.spark.DeltaSharingSparkSessionExtension")
    .config("spark.sql.catalog.delta_sharing", "io.delta.sharing.spark.DeltaSharingCatalog")
    .config("spark.sql.catalog.delta_sharing.shareCredentialsFile", "profile.share")
    .getOrCreate()
)

df = spark.read.table("delta_sharing.catalog.schema.table")
df.show()
```

The path `delta_sharing.catalog.schema.table` comes from `profile.share`.
Use `spark.sql('SHOW TABLES IN delta_sharing')` to list available tables.
**Note:** Delta Sharing does not support tables that use column mapping mode `name` or those with deletion vectors.

To run PySpark locally you must submit your script with the Delta Sharing jar:

```bash
spark-submit --packages io.delta:delta-sharing-spark_2.12:3.1.0 your_script.py
```

## Example scripts

Two runnable examples are included in the repository:

* `pyspark-scripts/delta_sharing_load.py` uses `spark.read.format("deltaSharing")` to load the table and select columns.
* `pyspark-scripts/delta_sharing_view.py` creates a temporary view using the Delta Sharing data source and queries it with SQL.

Each script expects the path to the `.share` file and table to be updated as needed.
