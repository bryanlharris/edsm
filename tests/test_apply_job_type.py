import sys
import types
import pathlib
import importlib.util
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

# Provide a minimal ``functions`` package to satisfy relative imports
pkg_path = pathlib.Path(__file__).resolve().parents[1] / 'functions'
functions_pkg = types.ModuleType('functions')
functions_pkg.__path__ = [str(pkg_path)]
sys.modules.setdefault('functions', functions_pkg)

# Import utility module dynamically
util_path = (
    pathlib.Path(__file__).resolve().parents[1] / 'functions' / 'utility.py'
)
spec = importlib.util.spec_from_file_location('functions.utility', util_path)
utility = importlib.util.module_from_spec(spec)
spec.loader.exec_module(utility)


class ApplyJobTypeTests(unittest.TestCase):
    def test_merges_silver_sample_batch_defaults(self):
        settings = {
            'job_type': 'silver_sample_batch',
            'simple_settings': 'true',
            'dst_table_name': 'cat.silver.tbl'
        }
        result = utility.apply_job_type(settings)
        expected = {
            'job_type': 'silver_sample_batch',
            'simple_settings': 'true',
            'dst_table_name': 'cat.silver.tbl',
            'read_function': 'functions.read.read_table',
            'transform_function': 'functions.transform.sample_table',
            'write_function': 'functions.write.overwrite_table',
            'build_history': 'false',
            'ingest_time_column': 'ingest_time',
        }
        self.assertEqual(result, expected)

    def test_user_values_override_defaults(self):
        settings = {
            'job_type': 'silver_sample_batch',
            'simple_settings': 'true',
            'dst_table_name': 'cat.silver.tbl',
            'write_function': 'custom.write'
        }
        result = utility.apply_job_type(settings)
        self.assertEqual(result['write_function'], 'custom.write')
        self.assertEqual(result['transform_function'], 'functions.transform.sample_table')


if __name__ == '__main__':
    unittest.main()
