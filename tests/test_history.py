import types
import pathlib
import importlib.util
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

func_mod = sys.modules['pyspark.sql.functions']
types_mod = sys.modules['pyspark.sql.types']
func_mod.col = lambda x: None
func_mod.lit = lambda x: None
func_mod.expr = lambda x: None
func_mod.current_timestamp = lambda: None
types_mod.StructType = type('StructType', (), {})

# Provide an empty functions package to satisfy relative imports
pkg_path = pathlib.Path(__file__).resolve().parents[1] / 'functions'
functions_pkg = types.ModuleType('functions')
functions_pkg.__path__ = [str(pkg_path)]
sys.modules.setdefault('functions', functions_pkg)

# Stub out functions.transform.add_row_hash to avoid pyspark dependencies
transform_mod = types.ModuleType('functions.transform')
transform_mod.add_row_hash = lambda df, cols, name='row_hash', use_row_hash=False: df
sys.modules['functions.transform'] = transform_mod

# Import history module dynamically
hist_path = pathlib.Path(__file__).resolve().parents[1] / 'functions' / 'history.py'
spec = importlib.util.spec_from_file_location('functions.history', hist_path)
history = importlib.util.module_from_spec(spec)
spec.loader.exec_module(history)

class DummyAgg:
    def __init__(self, value):
        self.value = value
    def agg(self, _):
        return self
    def collect(self):
        return [[self.value]]

class DummyCatalog:
    def __init__(self, exists):
        self.exists = exists
    def tableExists(self, name):
        return self.exists

class DummySpark:
    def __init__(self, exists=True, last_value=None, current_value=0):
        self.catalog = DummyCatalog(exists)
        self.last_value = last_value
        self.current_value = current_value
    def table(self, name):
        return DummyAgg(self.last_value)
    def sql(self, query):
        return DummyAgg(self.current_value)

class HistoryTests(unittest.TestCase):
    def test_handles_empty_file_version_table(self):
        spark = DummySpark(exists=True, last_value=None, current_value=0)
        history.describe_and_filter_history = lambda *a, **k: []
        history.build_and_merge_file_history('cat.sch.tbl', 'hist', spark)

    def test_history_pipeline_calls_build_when_schema_exists(self):
        calls = []
        history.build_and_merge_file_history = lambda full, schema, sp: calls.append((full, schema))
        history.schema_exists = lambda catalog, schema, sp: True
        spark = DummySpark()
        settings = {
            'build_history': 'true',
            'dst_table_name': 'cat.sch.tbl',
            'history_schema': 'hist',
        }
        history.history_pipeline(settings, spark)
        self.assertEqual(calls, [('cat.sch.tbl', 'hist')])

    def test_history_pipeline_skips_when_schema_missing(self):
        calls = []
        history.build_and_merge_file_history = lambda *a, **k: calls.append('called')
        history.schema_exists = lambda catalog, schema, sp: False
        spark = DummySpark()
        settings = {
            'build_history': 'true',
            'dst_table_name': 'cat.sch.tbl',
            'history_schema': 'hist',
        }
        history.history_pipeline(settings, spark)
        self.assertEqual(calls, [])

if __name__ == '__main__':
    unittest.main()
