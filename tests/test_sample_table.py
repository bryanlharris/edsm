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

for name in [
    'StructType', 'StructField', 'StringType', 'LongType',
    'TimestampType', 'ArrayType', 'MapType'
]:
    setattr(types_mod, name, type(name, (), {}))

class DummyColumn:
    def isNotNull(self):
        return self
    def __eq__(self, other):
        return self

dummy_col = DummyColumn()
captured = {}

func_mod.col = lambda name: dummy_col
func_mod.hash = lambda col: dummy_col
def pmod(col_obj, modulus):
    captured['modulus'] = modulus
    return dummy_col
func_mod.pmod = pmod
def dummy(*args, **kwargs):
    return dummy_col
for name in [
    'concat','regexp_extract','date_format','current_timestamp','when','col',
    'to_timestamp','to_date','regexp_replace','sha2','lit','trim','struct',
    'to_json','expr','transform','array','rand','conv','substring','hash','pmod'
]:
    func_mod.__dict__.setdefault(name, dummy)

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

transform_path = pathlib.Path(__file__).resolve().parents[1] / 'functions' / 'transform.py'
spec = importlib.util.spec_from_file_location('functions.transform', transform_path)
transform = importlib.util.module_from_spec(spec)
spec.loader.exec_module(transform)

class DummyDF:
    def __init__(self):
        self.wheres = 0
    def where(self, expr):
        self.wheres += 1
        return self

class DummySpark:
    def __init__(self, count=20):
        self.read = types.SimpleNamespace(table=lambda name: DummyDF())
        self.count = count
        self.query = None
    def sql(self, query):
        self.query = query
        return types.SimpleNamespace(collect=lambda: [[self.count]])

class SimpleSampleTests(unittest.TestCase):
    def test_simple_sampling_uses_modulus(self):
        spark = DummySpark(count=20)
        settings = {
            'sample_type': 'simple',
            'sample_id_col': 'id',
            'sample_size': '4',
            'src_table_name': 'tbl'
        }
        df = DummyDF()
        result = transform.sample_table(df, settings, spark=spark)
        self.assertIsInstance(result, DummyDF)
        self.assertEqual(result.wheres, 2)
        self.assertEqual(captured.get('modulus'), 5)
        self.assertEqual(
            spark.query,
            'SELECT approx_count_distinct(*) AS total FROM tbl'
        )

if __name__ == '__main__':
    unittest.main()
