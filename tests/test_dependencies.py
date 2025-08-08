import sys
import types
import pathlib
import importlib.util
import unittest

# Ensure repository root is on path
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

# Provide a minimal ``functions`` package to satisfy relative imports
pkg_path = pathlib.Path(__file__).resolve().parents[1] / 'functions'
functions_pkg = types.ModuleType('functions')
functions_pkg.__path__ = [str(pkg_path)]
sys.modules.setdefault('functions', functions_pkg)

# Import dependencies module dynamically
mod_path = pkg_path / 'dependencies.py'
spec = importlib.util.spec_from_file_location('functions.dependencies', mod_path)
dependencies = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dependencies)


class SortByDependencyTests(unittest.TestCase):
    def test_topological_sort(self):
        items = [
            {'table': 'b', 'requires': ['a']},
            {'table': 'c', 'requires': ['b']},
            {'table': 'a'},
        ]
        result = dependencies.sort_by_dependency(items)
        self.assertEqual([i['table'] for i in result], ['a', 'b', 'c'])

    def test_cycle_detection(self):
        items = [
            {'table': 'a', 'requires': ['b']},
            {'table': 'b', 'requires': ['a']},
        ]
        with self.assertRaises(ValueError):
            dependencies.sort_by_dependency(items)


if __name__ == '__main__':
    unittest.main()
