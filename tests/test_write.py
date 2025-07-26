import sys
import types
import pathlib
import importlib.util
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

functions_mod = sys.modules['pyspark.sql.functions']
window_mod = sys.modules['pyspark.sql.window']

# Stub out dependent functions package modules
utility_mod = types.ModuleType('functions.utility')
utility_mod.create_table_if_not_exists = lambda df, table, spark: None
utility_mod.get_function = lambda name: lambda *args, **kwargs: (lambda df, id: None)
transform_mod = types.ModuleType('functions.transform')
transform_mod.silver_scd2_transform = lambda df, settings, spark: df
sys.modules['functions.utility'] = utility_mod
sys.modules['functions.transform'] = transform_mod

# dummy row_number and Window
class DummyRowNumber:
    def over(self, window):
        return 'row_number()'

def row_number():
    return DummyRowNumber()

class DummyWindow:
    def partitionBy(*args, **kwargs):
        return DummyWindow()
    def orderBy(*args, **kwargs):
        return DummyWindow()

class DummyCol(str):
    def desc(self):
        return self

functions_mod.col = lambda x: DummyCol(x)
functions_mod.row_number = row_number
window_mod.Window = DummyWindow

# Load write module dynamically
write_path = pathlib.Path(__file__).resolve().parents[1] / 'functions' / 'write.py'
spec = importlib.util.spec_from_file_location('functions.write', write_path)
write = importlib.util.module_from_spec(spec)
spec.loader.exec_module(write)

class DummyDF:
    def __init__(self):
        self.calls = []
    def withColumn(self, *args, **kwargs):
        self.calls.append('withColumn')
        return self
    def filter(self, *args, **kwargs):
        self.calls.append('filter')
        return self
    def drop(self, *args, **kwargs):
        self.calls.append('drop')
        return self
    def dropDuplicates(self, *args, **kwargs):
        self.calls.append('dropDuplicates')
        return self
    def createOrReplaceTempView(self, name):
        self.calls.append(f'createOrReplaceTempView({name})')

class DummySpark:
    def __init__(self):
        self.queries = []
    def sql(self, query):
        self.queries.append(query)
        return None


class DummyStreamingQuery:
    def __init__(self):
        self.terminated = False

    def awaitTermination(self):
        self.terminated = True


class DummyWriteStream:
    def __init__(self, df):
        self.df = df
        self.query = None

    def format(self, fmt):
        return self

    def options(self, **kwargs):
        return self

    def outputMode(self, mode):
        return self

    def trigger(self, **kwargs):
        return self

    def table(self, name):
        self.query = DummyStreamingQuery()
        return self.query

    def foreachBatch(self, func):
        self.foreach_func = func
        return self

    def queryName(self, name):
        return self

    def start(self):
        self.query = DummyStreamingQuery()
        return self.query


class DummyStreamingDF(DummyDF):
    def __init__(self):
        super().__init__()
        self.writeStream = DummyWriteStream(self)

class SimpleMergeTests(unittest.TestCase):
    def test_dedup_by_ingest_time(self):
        df = DummyDF()
        spark = DummySpark()
        settings = {
            'dst_table_name': 't',
            'business_key': ['id'],
            'surrogate_key': ['val'],
            'ingest_time_column': 'ts',
        }
        write._simple_merge(df, settings, spark)
        self.assertEqual(df.calls[:3], ['withColumn', 'filter', 'drop'])
        self.assertIn('createOrReplaceTempView(updates)', df.calls)
        self.assertTrue(spark.queries)

    def test_drop_duplicates_when_no_ingest_time(self):
        df = DummyDF()
        spark = DummySpark()
        settings = {
            'dst_table_name': 't',
            'business_key': ['id'],
            'surrogate_key': ['val'],
        }
        write._simple_merge(df, settings, spark)
        self.assertIn('dropDuplicates', df.calls)
        self.assertIn('createOrReplaceTempView(updates)', df.calls)
        self.assertTrue(spark.queries)


class StreamingWriteTests(unittest.TestCase):
    def test_stream_write_table_returns_query(self):
        df = DummyStreamingDF()
        spark = DummySpark()
        settings = {
            'dst_table_name': 'tbl',
            'writeStreamOptions': {}
        }
        query = write.stream_write_table(df, settings, spark)
        self.assertFalse(query.terminated)

    def test_stream_upsert_table_returns_query(self):
        df = DummyStreamingDF()
        spark = DummySpark()
        settings = {
            'dst_table_name': 'tbl',
            'writeStreamOptions': {},
            'upsert_function': 'foo'
        }
        query = write.stream_upsert_table(df, settings, spark)
        self.assertFalse(query.terminated)

if __name__ == '__main__':
    unittest.main()
