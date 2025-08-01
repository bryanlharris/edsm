import sys
import types
import pathlib
import importlib.util
import unittest

func_mod = sys.modules['pyspark.sql.functions']
types_mod = sys.modules['pyspark.sql.types']

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
    def __init__(self, columns=None):
        self.wheres = 0
        self.columns = columns or []
        self.dropped = []

    def where(self, expr):
        self.wheres += 1
        return self

    def drop(self, *cols):
        self.dropped.extend(cols)
        self.columns = [c for c in self.columns if c not in cols]
        return self

class DummySpark:
    def __init__(self, count=20, exists=True):
        self.read = types.SimpleNamespace(table=lambda name: DummyDF())
        self.count = count
        self.query = None
        self.catalog = types.SimpleNamespace(tableExists=lambda name: exists)

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
            'SELECT approx_count_distinct(id) AS total FROM tbl'
        )

    def test_returns_input_when_table_missing(self):
        captured.clear()
        spark = DummySpark(exists=False)
        settings = {
            'sample_type': 'simple',
            'sample_id_col': 'id',
            'sample_size': '4',
            'src_table_name': 'tbl'
        }
        df = DummyDF()
        result = transform.sample_table(df, settings, spark=spark)
        self.assertIs(result, df)
        self.assertEqual(result.wheres, 0)
        self.assertIsNone(spark.query)
        self.assertNotIn('modulus', captured)

    def test_drops_rescued_data_column(self):
        spark = DummySpark()
        df = DummyDF(columns=['id', '_rescued_data'])
        settings = {
            'sample_type': 'simple',
            'sample_id_col': 'id',
            'sample_size': '1',
            'src_table_name': 'tbl'
        }
        result = transform.sample_table(df, settings, spark=spark)
        self.assertIs(result, df)
        self.assertIn('_rescued_data', df.dropped)

if __name__ == '__main__':
    unittest.main()
