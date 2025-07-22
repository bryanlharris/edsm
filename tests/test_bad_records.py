import unittest
import tempfile
import os
import types
import pathlib
import sys
import importlib.util

# Stub minimal pyspark modules required by functions.utility
pyspark = types.ModuleType('pyspark')
sql = types.ModuleType('pyspark.sql')
types_mod = types.ModuleType('pyspark.sql.types')
pyspark.sql = sql
sql.types = types_mod
types_mod.StructType = type('StructType', (), {})
sys.modules['pyspark'] = pyspark
sys.modules['pyspark.sql'] = sql
sys.modules['pyspark.sql.types'] = types_mod

# Create minimal functions package to satisfy relative imports in utility
pkg_path = pathlib.Path(__file__).resolve().parents[1] / 'functions'
functions_pkg = types.ModuleType('functions')
functions_pkg.__path__ = [str(pkg_path)]
sys.modules.setdefault('functions', functions_pkg)

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

# Import create_bad_records_table from functions.utility
util_path = pathlib.Path(__file__).resolve().parents[1] / 'functions' / 'utility.py'
spec = importlib.util.spec_from_file_location('functions.utility', util_path)
util = importlib.util.module_from_spec(spec)
spec.loader.exec_module(util)

class DummyWriter:
    def __init__(self, spark):
        self.spark = spark
    def mode(self, _):
        return self
    def format(self, _):
        return self
    def saveAsTable(self, name):
        self.spark.tables[name] = True

class DummyDF:
    def __init__(self, spark):
        self.write = DummyWriter(spark)

class DummySpark:
    def __init__(self):
        self.tables = {}
        self.commands = []
        self.read = types.SimpleNamespace(json=lambda paths: DummyDF(self))
        self.catalog = types.SimpleNamespace(tableExists=lambda name: name in self.tables)
    def sql(self, query):
        self.commands.append(query)

class BadRecordsTests(unittest.TestCase):
    def test_creates_table_when_files_exist(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, 'bad_records'))
            with open(os.path.join(tmp, 'bad_records', 'part-1'), 'w') as f:
                f.write('{"a":1}\n')
            settings = {
                'dst_table_name': 'cat.sch.tbl',
                'readStreamOptions': {'badRecordsPath': tmp}
            }
            spark = DummySpark()
            with self.assertRaises(Exception):
                util.create_bad_records_table(settings, spark)
            self.assertIn('cat.sch.tbl_bad_records', spark.tables)

    def test_drops_table_when_no_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = {
                'dst_table_name': 'cat.sch.tbl',
                'readStreamOptions': {'badRecordsPath': tmp}
            }
            spark = DummySpark()
            util.create_bad_records_table(settings, spark)
            self.assertNotIn('cat.sch.tbl_bad_records', spark.tables)
            self.assertIn('DROP TABLE IF EXISTS cat.sch.tbl_bad_records', spark.commands)

if __name__ == '__main__':
    unittest.main()
