import sys
import types
import pathlib
import importlib.util
import unittest

func_mod = sys.modules['pyspark.sql.functions']
types_mod = sys.modules['pyspark.sql.types']
window_mod = types.ModuleType('pyspark.sql.window')
setattr(window_mod, 'Window', type('Window', (), {}))
sys.modules['pyspark.sql.window'] = window_mod

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
    def __lt__(self, other):
        captured['threshold'] = other
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
    'to_json','expr','transform','array','rand','conv','substring','hash','pmod','row_number'
]:
    func_mod.__dict__.setdefault(name, dummy)

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

transform_path = pathlib.Path(__file__).resolve().parents[1] / 'functions' / 'transform.py'
spec = importlib.util.spec_from_file_location('functions.transform', transform_path)
transform = importlib.util.module_from_spec(spec)
spec.loader.exec_module(transform)

def add_row_hash_mod(df, column_name, modulus):
    captured['modulus'] = modulus
    return df

transform.add_row_hash_mod = add_row_hash_mod

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

    def transform(self, func, *args, **kwargs):
        return func(self, *args, **kwargs)

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
    def setUp(self):
        captured.clear()
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
            'SELECT count(*) AS total FROM tbl'
        )

    def test_simple_sampling_fraction_skips_source_count(self):
        spark = DummySpark(count=20)
        settings = {
            'sample_type': 'simple',
            'sample_id_col': 'id',
            'sample_fraction': 0.2,
        }
        df = DummyDF()
        result = transform.sample_table(df, settings, spark=spark)
        self.assertIs(result, df)
        self.assertEqual(result.wheres, 2)
        self.assertEqual(captured.get('modulus'), 5)
        self.assertIsNone(spark.query)

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

    def test_simple_sampling_requires_parameter(self):
        spark = DummySpark()
        settings = {
            'sample_type': 'simple',
            'sample_id_col': 'id',
        }
        df = DummyDF()
        with self.assertRaises(ValueError):
            transform.sample_table(df, settings, spark=spark)

    def test_simple_sampling_parameters_mutually_exclusive(self):
        spark = DummySpark()
        settings = {
            'sample_type': 'simple',
            'sample_id_col': 'id',
            'sample_fraction': 0.1,
            'sample_size': '10',
        }
        df = DummyDF()
        with self.assertRaises(ValueError):
            transform.sample_table(df, settings, spark=spark)


class DeterministicSampleTests(unittest.TestCase):
    def setUp(self):
        captured.clear()

    def test_fraction_threshold_and_modulus(self):
        spark = DummySpark()
        settings = {
            'sample_type': 'deterministic',
            'sample_fraction': 0.2,
            'hash_modulus': '10',
        }
        df = DummyDF(columns=['row_hash'])
        result = transform.sample_table(df, settings, spark=spark)
        self.assertIs(result, df)
        self.assertEqual(captured.get('modulus'), 10)
        self.assertEqual(captured.get('threshold'), 2)
        self.assertIn('row_hash_mod', df.dropped)

    def test_fraction_si_notation_raises(self):
        spark = DummySpark()
        settings = {
            'sample_type': 'deterministic',
            'sample_fraction': '500k',
            'hash_modulus': '1M',
        }
        df = DummyDF(columns=['row_hash'])
        with self.assertRaises(ValueError):
            transform.sample_table(df, settings, spark=spark)

    def test_uses_sample_size(self):
        spark = DummySpark()
        settings = {
            'sample_type': 'deterministic',
            'sample_size': '10k',
            'hash_modulus': '1M',
        }
        df = DummyDF(columns=['row_hash'])
        transform.sample_table(df, settings, spark=spark)
        self.assertEqual(captured.get('modulus'), 1000000)
        self.assertEqual(captured.get('threshold'), 10000)

    def test_requires_parameter(self):
        spark = DummySpark()
        settings = {
            'sample_type': 'deterministic',
            'hash_modulus': '10',
        }
        df = DummyDF(columns=['row_hash'])
        with self.assertRaises(ValueError):
            transform.sample_table(df, settings, spark=spark)

    def test_parameters_mutually_exclusive(self):
        spark = DummySpark()
        settings = {
            'sample_type': 'deterministic',
            'sample_fraction': 0.1,
            'sample_size': '10k',
            'hash_modulus': '1M',
        }
        df = DummyDF(columns=['row_hash'])
        with self.assertRaises(ValueError):
            transform.sample_table(df, settings, spark=spark)

    def test_fraction_range(self):
        spark = DummySpark()
        settings = {
            'sample_type': 'deterministic',
            'sample_fraction': 1.2,
            'hash_modulus': '10',
        }
        df = DummyDF(columns=['row_hash'])
        with self.assertRaises(ValueError):
            transform.sample_table(df, settings, spark=spark)

if __name__ == '__main__':
    unittest.main()
