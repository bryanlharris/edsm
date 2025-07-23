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

## `utilities/delta_sharing_example.py`

A small script in the repository performs the same setup. Pass the credential
file and table path as optional arguments.

```bash
python utilities/delta_sharing_example.py profile.share delta_sharing.catalog.schema.table
```

