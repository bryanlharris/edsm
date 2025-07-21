import sys
import types
import pathlib
from unittest import mock
import importlib.util
import unittest

# Create minimal fake pyspark modules before importing transform
pyspark = types.ModuleType('pyspark')
sql = types.ModuleType('pyspark.sql')
types_mod = types.ModuleType('pyspark.sql.types')
func_mod = types.ModuleType('pyspark.sql.functions')
window_mod = types.ModuleType('pyspark.sql.window')
sql.types = types_mod
sql.functions = func_mod
pyspark.sql = sql
sys.modules['pyspark'] = pyspark
sys.modules['pyspark.sql'] = sql
sys.modules['pyspark.sql.types'] = types_mod
sys.modules['pyspark.sql.functions'] = func_mod
sys.modules['pyspark.sql.window'] = window_mod

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

# Provide dummy classes/functions referenced in transform
for name in [
    'StructType', 'StructField', 'StringType', 'LongType',
    'TimestampType', 'ArrayType', 'MapType'
]:
    setattr(types_mod, name, type(name, (), {}))

def dummy(*args, **kwargs):
    return None
for name in [
    'concat','regexp_extract','date_format','current_timestamp','when','col',
    'to_timestamp','to_date','regexp_replace','sha2','lit','trim','struct',
    'to_json','expr','transform','array','rand','conv','substring','hash','pmod'
]:
    setattr(func_mod, name, dummy)
func_mod.row_number = dummy
window_mod.Window = type('Window', (), {})

# Import the module under test after faking pyspark
transform_path = pathlib.Path(__file__).resolve().parents[1] / 'functions' / 'transform.py'
spec = importlib.util.spec_from_file_location('functions.transform', transform_path)
transform = importlib.util.module_from_spec(spec)
spec.loader.exec_module(transform)

class DummyDF:
    def __init__(self, columns=None):
        self.columns = columns or []
        self.calls = []

    def transform(self, func, *args, **kwargs):
        self.calls.append(func.__name__)
        return self

    def withColumn(self, *args, **kwargs):
        self.calls.append('withColumn')
        return self

class BronzeTransformTests(unittest.TestCase):
    def test_transform_does_not_call_add_rescued_data(self):
        df = DummyDF(['foo'])
        settings = {
            'file_schema': [
                {'name': 'foo', 'type': 'string'},
                {'name': '_rescued_data', 'type': 'string'}
            ],
            'use_metadata': 'true'
        }
        with mock.patch.object(transform, 'clean_column_names', new=lambda df_in: df_in), \
             mock.patch.object(transform, 'add_source_metadata', new=lambda df_in, settings: df_in):
            transform.bronze_standard_transform(df, settings, spark=None)
            self.assertNotIn('add_rescued_data', df.calls)


if __name__ == '__main__':
    unittest.main()
