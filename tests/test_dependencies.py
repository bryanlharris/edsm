import sys
import types
import pathlib
import pytest

# Ensure repository root is on path
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

# Provide a minimal ``functions`` package to satisfy relative imports
pkg_path = pathlib.Path(__file__).resolve().parents[1] / 'functions'
functions_pkg = types.ModuleType('functions')
functions_pkg.__path__ = [str(pkg_path)]
sys.modules.setdefault('functions', functions_pkg)

from functions.dependencies import sort_by_dependency


def test_topological_sort():
    items = [
        {'table': 'b', 'requires': ['a']},
        {'table': 'c', 'requires': ['b']},
        {'table': 'a'},
    ]
    result = sort_by_dependency(items)
    assert [i['table'] for i in result] == ['a', 'b', 'c']


def test_cycle_detection():
    items = [
        {'table': 'a', 'requires': ['b']},
        {'table': 'b', 'requires': ['a']},
    ]
    with pytest.raises(ValueError):
        sort_by_dependency(items)
