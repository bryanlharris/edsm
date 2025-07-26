import sys
import types
import pytest

# Create and register minimal pyspark modules so tests can import project
# modules without having pyspark installed.
pyspark = types.ModuleType("pyspark")
sql = types.ModuleType("pyspark.sql")
types_mod = types.ModuleType("pyspark.sql.types")
functions_mod = types.ModuleType("pyspark.sql.functions")
window_mod = types.ModuleType("pyspark.sql.window")

sql.types = types_mod
sql.functions = functions_mod
pyspark.sql = sql

types_mod.StructType = type("StructType", (), {})

_MODULES = {
    "pyspark": pyspark,
    "pyspark.sql": sql,
    "pyspark.sql.types": types_mod,
    "pyspark.sql.functions": functions_mod,
    "pyspark.sql.window": window_mod,
}

for name, mod in _MODULES.items():
    sys.modules.setdefault(name, mod)


@pytest.fixture(autouse=True)
def fake_pyspark_modules():
    """Yield stub modules and clean them up after the test session."""
    yield _MODULES
