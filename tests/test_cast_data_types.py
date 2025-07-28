import sys
import types
import pathlib
import importlib.util
import unittest

func_mod = sys.modules['pyspark.sql.functions']
types_mod = sys.modules['pyspark.sql.types']

# Provide placeholder classes for required pyspark types
for name in [
    'StructType', 'StructField', 'StringType', 'LongType',
    'TimestampType', 'ArrayType', 'MapType'
]:
    setattr(types_mod, name, type(name, (), {}))

casts = []

class DummyColumn:
    def __init__(self, name=None):
        self.name = name
        self.alias_name = None
    def cast(self, dtype):
        casts.append((self.name, dtype))
        return self
    def alias(self, name):
        self.alias_name = name
        return self
    def rlike(self, pattern):
        return self
    def when(self, *args, **kwargs):
        return self
    def otherwise(self, *args, **kwargs):
        return self


def dummy(*args, **kwargs):
    return DummyColumn()

func_mod.col = lambda name: DummyColumn(name)
for name in [
    'concat','regexp_extract','date_format','current_timestamp','when','to_timestamp',
    'to_date','regexp_replace','sha2','lit','trim','struct','to_json','expr','transform',
    'array','rand','conv','substring','hash','pmod'
]:
    func_mod.__dict__[name] = dummy

# Helpers that should return their first Column argument
func_mod.regexp_replace = lambda col_obj, *a: col_obj
func_mod.to_date = lambda col_obj, *a, **k: col_obj
func_mod.to_timestamp = lambda col_obj, *a, **k: col_obj

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

# Ensure a minimal 'functions' package exists to satisfy relative imports
pkg_path = pathlib.Path(__file__).resolve().parents[1] / 'functions'
functions_pkg = types.ModuleType('functions')
functions_pkg.__path__ = [str(pkg_path)]
sys.modules.setdefault('functions', functions_pkg)

transform_path = pathlib.Path(__file__).resolve().parents[1] / 'functions' / 'transform.py'
spec = importlib.util.spec_from_file_location('functions.transform', transform_path)
transform = importlib.util.module_from_spec(spec)
spec.loader.exec_module(transform)

class DummyDF:
    def __init__(self, columns):
        self.columns = columns
        self.selected_names = None
    def select(self, cols):
        self.selected_names = [getattr(c, 'alias_name', None) or c.name for c in cols]
        return self


class CastDataTypeTests(unittest.TestCase):
    def setUp(self):
        casts.clear()

    def test_casts_specified_types_and_preserves_columns(self):
        df = DummyDF(['a', 'b', 'c'])
        data_map = {'a': 'integer', 'b': 'decimal(10,2)'}
        result = transform.cast_data_types(df, data_map)
        self.assertIs(result, df)
        self.assertEqual(df.selected_names, ['a', 'b', 'c'])
        self.assertIn(('a', 'integer'), casts)
        self.assertIn(('b', 'decimal(10,2)'), casts)

    def test_unknown_types_are_ignored(self):
        df = DummyDF(['x', 'y'])
        data_map = {'x': 'mystery'}
        result = transform.cast_data_types(df, data_map)
        self.assertIs(result, df)
        self.assertEqual(df.selected_names, ['x', 'y'])
        self.assertEqual(casts, [])

if __name__ == '__main__':
    unittest.main()
