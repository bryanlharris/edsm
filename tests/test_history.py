import types
import pathlib
import importlib.util
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

# Prepare dummy pyspark modules before importing history
pyspark = types.ModuleType('pyspark')
sql = types.ModuleType('pyspark.sql')
types_mod = types.ModuleType('pyspark.sql.types')
func_mod = types.ModuleType('pyspark.sql.functions')
pyspark.sql = sql
sql.types = types_mod
sql.functions = func_mod
func_mod.col = lambda x: None
func_mod.lit = lambda x: None
func_mod.expr = lambda x: None
func_mod.current_timestamp = lambda: None
func_mod.sha2 = lambda *a, **k: None
func_mod.to_json = lambda *a, **k: None
func_mod.struct = lambda *a, **k: None
types_mod.StructType = type('StructType', (), {})
sys.modules['pyspark'] = pyspark
sys.modules['pyspark.sql'] = sql
sys.modules['pyspark.sql.functions'] = func_mod
sys.modules['pyspark.sql.types'] = types_mod

# Provide an empty functions package to satisfy relative imports
pkg_path = pathlib.Path(__file__).resolve().parents[1] / 'functions'
functions_pkg = types.ModuleType('functions')
functions_pkg.__path__ = [str(pkg_path)]
sys.modules.setdefault('functions', functions_pkg)

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

if __name__ == '__main__':
    unittest.main()
