import sys
import types
import pathlib
import importlib.util
import unittest

# Stub pyspark modules
pyspark = types.ModuleType('pyspark')
sql = types.ModuleType('pyspark.sql')
func_mod = types.ModuleType('pyspark.sql.functions')
types_mod = types.ModuleType('pyspark.sql.types')
sql.functions = func_mod
sql.types = types_mod
pyspark.sql = sql
sys.modules['pyspark'] = pyspark
sys.modules['pyspark.sql'] = sql
sys.modules['pyspark.sql.functions'] = func_mod
sys.modules['pyspark.sql.types'] = types_mod

# Provide placeholder classes for required pyspark types
for name in [
    'StructType', 'StructField', 'StringType', 'LongType',
    'TimestampType', 'ArrayType', 'MapType'
]:
    setattr(types_mod, name, type(name, (), {}))

class DummyColumn:
    def __init__(self):
        self.casts = []
    def cast(self, dtype):
        self.casts.append(dtype)
        return self
    def __mod__(self, other):
        return self

dummy_col = DummyColumn()
func_mod.col = lambda name: dummy_col
func_mod.substring = lambda col, start, length: dummy_col
func_mod.conv = lambda val, from_base, to_base: dummy_col
for name in [
    'concat','regexp_extract','date_format','current_timestamp','when','col',
    'to_timestamp','to_date','regexp_replace','sha2','lit','trim','struct',
    'to_json','expr','transform','array','rand','conv','substring'
]:
    func_mod.__dict__.setdefault(name, lambda *a, **k: dummy_col)

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

transform_path = pathlib.Path(__file__).resolve().parents[1] / 'functions' / 'transform.py'
spec = importlib.util.spec_from_file_location('functions.transform', transform_path)
transform = importlib.util.module_from_spec(spec)
spec.loader.exec_module(transform)

class DummyDF:
    def withColumn(self, name, expr):
        return self

class RowHashModTests(unittest.TestCase):
    def test_add_row_hash_mod_uses_decimal(self):
        df = DummyDF()
        transform.add_row_hash_mod(df, 'hash', 100)
        self.assertIn('decimal(38,0)', dummy_col.casts)
        self.assertIn('long', dummy_col.casts)

if __name__ == '__main__':
    unittest.main()
