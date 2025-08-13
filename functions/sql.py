# Functions for executing SQL notebooks within the pipeline


def run_sql_notebook(settings, spark):
    """Execute a SQL notebook.

    Parameters
    ----------
    settings : dict
        Configuration dictionary expected to contain ``notebook_path``.
    spark : pyspark.sql.SparkSession
        Provided for interface compatibility; unused directly here.
    """
    dbutils.notebook.run(settings["notebook_path"], 0)
