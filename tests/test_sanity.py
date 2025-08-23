import sys
import types
import pathlib
import pytest

types_mod = sys.modules['pyspark.sql.types']
for name in [
    'StructType', 'StructField', 'StringType', 'LongType',
    'TimestampType', 'ArrayType', 'MapType'
]:
    setattr(types_mod, name, type(name, (), {}))
types_mod.__getattr__ = lambda name: type(name, (), {})

func_mod = sys.modules['pyspark.sql.functions']
for name in [
    'col', 'rand', 'pmod', 'hash', 'row_number',
    'concat', 'regexp_extract', 'date_format', 'current_timestamp', 'when'
]:
    setattr(func_mod, name, lambda *a, **k: None)
func_mod.__getattr__ = lambda name: (lambda *a, **k: None)

window_mod = types.ModuleType('pyspark.sql.window')
setattr(window_mod, 'Window', type('Window', (), {}))
sys.modules['pyspark.sql.window'] = window_mod

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from functions import sanity, config


def test_no_warning_when_slash_present(capsys, monkeypatch):
    monkeypatch.setattr(config, 'S3_ROOT_LANDING', 's3://landing/')
    monkeypatch.setattr(config, 'S3_ROOT_UTILITY', 's3://utility/')
    sanity.validate_s3_roots()
    out = capsys.readouterr().out
    assert out == ''
    assert config.S3_ROOT_LANDING.endswith('/')
    assert config.S3_ROOT_UTILITY.endswith('/')


def test_append_slash_and_warn(capsys, monkeypatch):
    monkeypatch.setattr(config, 'S3_ROOT_LANDING', 's3://landing')
    monkeypatch.setattr(config, 'S3_ROOT_UTILITY', 's3://utility')
    sanity.validate_s3_roots()
    out = capsys.readouterr().out
    assert 'WARNING' in out
    assert config.S3_ROOT_LANDING == 's3://landing/'
    assert config.S3_ROOT_UTILITY == 's3://utility/'


class DummyDbutils:
    class Jobs:
        class TaskValues:
            def __init__(self, values=None):
                self.values = values or {}

            def get(self, taskKey=None, key=None):  # pragma: no cover - simple dict lookup
                return self.values.get(key)

        def __init__(self, values=None):
            self.taskValues = self.TaskValues(values)

    def __init__(self, values=None):
        self.jobs = self.Jobs(values)


def test_validate_settings_runs_s3_validation(capsys, monkeypatch):
    monkeypatch.setattr(config, 'S3_ROOT_LANDING', 's3://landing')
    monkeypatch.setattr(config, 'S3_ROOT_UTILITY', 's3://utility')

    monkeypatch.setattr(sanity, '_discover_settings_files', lambda: ({}, {}, {}, {}))
    sanity.validate_settings(DummyDbutils())

    out = capsys.readouterr().out
    assert 'Sanity check: Validate settings check passed.' in out
    assert 'WARNING' in out
    assert config.S3_ROOT_LANDING.endswith('/')
    assert config.S3_ROOT_UTILITY.endswith('/')


def test_initialize_schemas_warns_for_missing_history_schema(capsys, monkeypatch):
    path = 'dummy.json'
    monkeypatch.setattr(sanity, '_discover_settings_files', lambda: ({'tbl': path}, {}, {}, {}))

    import builtins, io, json, types

    def fake_open(p, *a, **k):
        if p == path:
            return io.StringIO(json.dumps({'dst_table_name': 'cat.sch.tbl', 'build_history': 'true', 'history_schema': 'hist'}))
        return builtins.open(p, *a, **k)

    monkeypatch.setattr(builtins, 'open', fake_open)
    monkeypatch.setattr(sanity, 'catalog_exists', lambda c, sp: True)
    monkeypatch.setattr(sanity, 'schema_exists', lambda c, s, sp: False)
    monkeypatch.setattr(sanity, 'create_schema_if_not_exists', lambda c, s, sp: print(f"\tINFO: Schema did not exist and was created: {c}.{s}."))
    monkeypatch.setattr(sanity, 'create_volume_if_not_exists', lambda c, s, v, sp: None)

    spark = types.SimpleNamespace(sql=lambda q: None)
    sanity.initialize_schemas_and_volumes(spark)
    out = capsys.readouterr().out
    assert 'WARNING: History schema does not exist: cat.hist' in out
    assert 'Initialize schemas and volumes check completed with warnings.' in out


def test_initialize_schemas_history_schema_exists(capsys, monkeypatch):
    path = 'dummy.json'
    monkeypatch.setattr(sanity, '_discover_settings_files', lambda: ({'tbl': path}, {}, {}, {}))

    import builtins, io, json, types

    def fake_open(p, *a, **k):
        if p == path:
            return io.StringIO(json.dumps({'dst_table_name': 'cat.sch.tbl', 'build_history': 'true', 'history_schema': 'hist'}))
        return builtins.open(p, *a, **k)

    monkeypatch.setattr(builtins, 'open', fake_open)
    monkeypatch.setattr(sanity, 'catalog_exists', lambda c, sp: True)
    monkeypatch.setattr(sanity, 'schema_exists', lambda c, s, sp: True)
    monkeypatch.setattr(sanity, 'create_schema_if_not_exists', lambda c, s, sp: None)
    monkeypatch.setattr(sanity, 'create_volume_if_not_exists', lambda c, s, v, sp: None)

    spark = types.SimpleNamespace(sql=lambda q: None)
    sanity.initialize_schemas_and_volumes(spark)
    out = capsys.readouterr().out
    assert 'WARNING' not in out
    assert 'Initialize schemas and volumes check passed.' in out


def test_validate_settings_skips_pipeline_function(capsys, monkeypatch):
    path = 'dummy.json'
    monkeypatch.setattr(sanity, '_discover_settings_files', lambda: ({}, { 'tbl': path }, {}, {}))

    import builtins, io, json

    def fake_open(p, *a, **k):
        if p == path:
            return io.StringIO(json.dumps({'pipeline_function': 'custom.pipeline'}))
        return builtins.open(p, *a, **k)

    monkeypatch.setattr(builtins, 'open', fake_open)
    monkeypatch.setattr(config, 'S3_ROOT_LANDING', 's3://landing/')
    monkeypatch.setattr(config, 'S3_ROOT_UTILITY', 's3://utility/')

    sanity.validate_settings(DummyDbutils())
    out = capsys.readouterr().out
    assert 'Sanity check: Validate settings check passed.' in out


def test_validate_settings_accepts_gold_sql_notebook(capsys, monkeypatch):
    path = 'gold.json'
    monkeypatch.setattr(sanity, '_discover_settings_files', lambda: ({}, {}, {}, {'tbl': path}))

    import builtins, io, json

    def fake_open(p, *a, **k):
        if p == path:
            return io.StringIO(json.dumps({
                'simple_settings': 'true',
                'job_type': 'gold_sql_notebook',
                'notebook_path': '/Workspace/Shared/example'
            }))
        return builtins.open(p, *a, **k)

    monkeypatch.setattr(builtins, 'open', fake_open)
    sanity.validate_settings(DummyDbutils())
    out = capsys.readouterr().out
    assert 'Sanity check: Validate settings check passed.' in out


def test_validate_settings_gold_sql_notebook_layer_mismatch(monkeypatch):
    path = 'silver.json'
    monkeypatch.setattr(sanity, '_discover_settings_files', lambda: ({}, {'tbl': path}, {}, {}))

    import builtins, io, json

    def fake_open(p, *a, **k):
        if p == path:
            return io.StringIO(json.dumps({
                'simple_settings': 'true',
                'job_type': 'gold_sql_notebook',
                'notebook_path': '/Workspace/Shared/example'
            }))
        return builtins.open(p, *a, **k)

    monkeypatch.setattr(builtins, 'open', fake_open)
    with pytest.raises(RuntimeError) as exc:
        sanity.validate_settings(DummyDbutils())
    assert 'gold_sql_notebook only allowed in gold layer' in str(exc.value)


def test_validate_settings_missing_dependency(monkeypatch):
    paths = {'a': 'a.json', 'b': 'b.json'}
    monkeypatch.setattr(sanity, '_discover_settings_files', lambda: ({}, paths, {}, {}))

    import builtins, io, json

    def fake_open(p, *a, **k):
        if p == 'a.json':
            return io.StringIO(json.dumps({
                'read_function': 'r',
                'transform_function': 't',
                'write_function': 'w',
                'src_table_name': 's',
                'dst_table_name': 'a'
            }))
        if p == 'b.json':
            return io.StringIO(json.dumps({
                'read_function': 'r',
                'transform_function': 't',
                'write_function': 'w',
                'src_table_name': 'a',
                'dst_table_name': 'b'
            }))
        return builtins.open(p, *a, **k)

    monkeypatch.setattr(builtins, 'open', fake_open)
    monkeypatch.setattr(config, 'S3_ROOT_LANDING', 's3://landing/')
    monkeypatch.setattr(config, 'S3_ROOT_UTILITY', 's3://utility/')

    dbutils = DummyDbutils({'silver_parallel': [], 'silver_sequential': [{'table': 'b', 'requires': ['missing']}]} )
    with pytest.raises(RuntimeError) as exc:
        sanity.validate_settings(dbutils)
    assert 'requires missing silver table missing' in str(exc.value)


def test_validate_settings_case_mismatch(monkeypatch):
    paths = {'a': 'a.json', 'b': 'b.json'}
    monkeypatch.setattr(sanity, '_discover_settings_files', lambda: ({}, paths, {}, {}))

    import builtins, io, json

    def fake_open(p, *a, **k):
        if p == 'a.json':
            return io.StringIO(json.dumps({
                'read_function': 'r',
                'transform_function': 't',
                'write_function': 'w',
                'src_table_name': 's',
                'dst_table_name': 'CAT.SCHEMA.TABLEA'
            }))
        if p == 'b.json':
            return io.StringIO(json.dumps({
                'read_function': 'r',
                'transform_function': 't',
                'write_function': 'w',
                'src_table_name': 'cat.schema.tablea',
                'dst_table_name': 'cat.schema.tableb'
            }))
        return builtins.open(p, *a, **k)

    monkeypatch.setattr(builtins, 'open', fake_open)
    monkeypatch.setattr(config, 'S3_ROOT_LANDING', 's3://landing/')
    monkeypatch.setattr(config, 'S3_ROOT_UTILITY', 's3://utility/')

    dbutils = DummyDbutils({'silver_parallel': [], 'silver_sequential': []})
    with pytest.raises(RuntimeError) as exc:
        sanity.validate_settings(dbutils)
    assert 'ensure consistent casing' in str(exc.value)


def test_validate_settings_detects_cycle(monkeypatch):
    paths = {'a': 'a.json', 'b': 'b.json'}
    monkeypatch.setattr(sanity, '_discover_settings_files', lambda: ({}, paths, {}, {}))

    import builtins, io, json

    def fake_open(p, *a, **k):
        if p == 'a.json':
            return io.StringIO(json.dumps({
                'read_function': 'r',
                'transform_function': 't',
                'write_function': 'w',
                'src_table_name': 'b',
                'dst_table_name': 'a'
            }))
        if p == 'b.json':
            return io.StringIO(json.dumps({
                'read_function': 'r',
                'transform_function': 't',
                'write_function': 'w',
                'src_table_name': 'a',
                'dst_table_name': 'b'
            }))
        return builtins.open(p, *a, **k)

    monkeypatch.setattr(builtins, 'open', fake_open)
    monkeypatch.setattr(config, 'S3_ROOT_LANDING', 's3://landing/')
    monkeypatch.setattr(config, 'S3_ROOT_UTILITY', 's3://utility/')

    deps = [
        {'table': 'a', 'requires': ['b']},
        {'table': 'b', 'requires': ['a']},
    ]
    dbutils = DummyDbutils({'silver_parallel': [], 'silver_sequential': deps})
    with pytest.raises(RuntimeError) as exc:
        sanity.validate_settings(dbutils)
    assert 'Circular dependency detected' in str(exc.value)


def test_validate_settings_conflicting_sample_keys(monkeypatch):
    path = 'sample.json'
    monkeypatch.setattr(
        sanity, '_discover_settings_files', lambda: ({}, {'tbl': path}, {}, {})
    )

    import builtins, io, json

    def fake_open(p, *a, **k):
        if p == path:
            return io.StringIO(
                json.dumps(
                    {
                        'read_function': 'r',
                        'transform_function': 't',
                        'write_function': 'w',
                        'src_table_name': 's',
                        'dst_table_name': 'd',
                        'sample_fraction': 0.1,
                        'sample_size': '1k',
                    }
                )
            )
        return builtins.open(p, *a, **k)

    monkeypatch.setattr(builtins, 'open', fake_open)
    dbutils = DummyDbutils({'silver_parallel': [], 'silver_sequential': []})
    with pytest.raises(RuntimeError) as exc:
        sanity.validate_settings(dbutils)
    assert 'sample_fraction or sample_size' in str(exc.value)


def test_validate_settings_simple_missing_sampling(monkeypatch):
    path = 'sample.json'
    monkeypatch.setattr(
        sanity, '_discover_settings_files', lambda: ({}, {'tbl': path}, {}, {})
    )

    import builtins, io, json

    def fake_open(p, *a, **k):
        if p == path:
            return io.StringIO(
                json.dumps(
                    {
                        'read_function': 'r',
                        'transform_function': 't',
                        'write_function': 'w',
                        'src_table_name': 's',
                        'dst_table_name': 'd',
                        'sample_type': 'simple',
                        'sample_id_col': 'id',
                    }
                )
            )
        return builtins.open(p, *a, **k)

    monkeypatch.setattr(builtins, 'open', fake_open)
    dbutils = DummyDbutils({'silver_parallel': [], 'silver_sequential': []})
    with pytest.raises(RuntimeError) as exc:
        sanity.validate_settings(dbutils)
    assert "sample_type 'simple' requires" in str(exc.value)


def test_validate_settings_simple_both_sampling(monkeypatch):
    path = 'sample.json'
    monkeypatch.setattr(
        sanity, '_discover_settings_files', lambda: ({}, {'tbl': path}, {}, {})
    )

    import builtins, io, json

    def fake_open(p, *a, **k):
        if p == path:
            return io.StringIO(
                json.dumps(
                    {
                        'read_function': 'r',
                        'transform_function': 't',
                        'write_function': 'w',
                        'src_table_name': 's',
                        'dst_table_name': 'd',
                        'sample_type': 'simple',
                        'sample_id_col': 'id',
                        'sample_fraction': 0.1,
                        'sample_size': '1k',
                    }
                )
            )
        return builtins.open(p, *a, **k)

    monkeypatch.setattr(builtins, 'open', fake_open)
    dbutils = DummyDbutils({'silver_parallel': [], 'silver_sequential': []})
    with pytest.raises(RuntimeError) as exc:
        sanity.validate_settings(dbutils)
    assert 'sample_fraction or sample_size' in str(exc.value)


def test_validate_settings_simple_fraction_only(monkeypatch, capsys):
    path = 'sample.json'
    monkeypatch.setattr(
        sanity, '_discover_settings_files', lambda: ({}, {'tbl': path}, {}, {})
    )

    import builtins, io, json

    def fake_open(p, *a, **k):
        if p == path:
            return io.StringIO(
                json.dumps(
                    {
                        'read_function': 'r',
                        'transform_function': 't',
                        'write_function': 'w',
                        'src_table_name': 's',
                        'dst_table_name': 'd',
                        'sample_type': 'simple',
                        'sample_id_col': 'id',
                        'sample_fraction': 0.1,
                    }
                )
            )
        return builtins.open(p, *a, **k)

    monkeypatch.setattr(builtins, 'open', fake_open)
    monkeypatch.setattr(config, 'S3_ROOT_LANDING', 's3://landing/')
    monkeypatch.setattr(config, 'S3_ROOT_UTILITY', 's3://utility/')
    sanity.validate_settings(DummyDbutils({'silver_parallel': [], 'silver_sequential': []}))
    out = capsys.readouterr().out
    assert 'Sanity check: Validate settings check passed.' in out


def test_validate_settings_deterministic_requires_sampling(monkeypatch):
    path = 'sample.json'
    monkeypatch.setattr(
        sanity, '_discover_settings_files', lambda: ({}, {'tbl': path}, {}, {})
    )

    import builtins, io, json

    def fake_open(p, *a, **k):
        if p == path:
            return io.StringIO(
                json.dumps(
                    {
                        'read_function': 'r',
                        'transform_function': 't',
                        'write_function': 'w',
                        'src_table_name': 's',
                        'dst_table_name': 'd',
                        'sample_type': 'deterministic',
                    }
                )
            )
        return builtins.open(p, *a, **k)

    monkeypatch.setattr(builtins, 'open', fake_open)
    dbutils = DummyDbutils({'silver_parallel': [], 'silver_sequential': []})
    with pytest.raises(RuntimeError) as exc:
        sanity.validate_settings(dbutils)
    assert "sample_type 'deterministic' requires" in str(exc.value)


def test_validate_settings_deterministic_exactly_one(monkeypatch, capsys):
    path = 'sample.json'
    monkeypatch.setattr(
        sanity, '_discover_settings_files', lambda: ({}, {'tbl': path}, {}, {})
    )

    import builtins, io, json

    def fake_open(p, *a, **k):
        if p == path:
            return io.StringIO(
                json.dumps(
                    {
                        'read_function': 'r',
                        'transform_function': 't',
                        'write_function': 'w',
                        'src_table_name': 's',
                        'dst_table_name': 'd',
                        'sample_type': 'deterministic',
                        'sample_fraction': 0.1,
                    }
                )
            )
        return builtins.open(p, *a, **k)

    monkeypatch.setattr(builtins, 'open', fake_open)
    monkeypatch.setattr(config, 'S3_ROOT_LANDING', 's3://landing/')
    monkeypatch.setattr(config, 'S3_ROOT_UTILITY', 's3://utility/')
    sanity.validate_settings(DummyDbutils({'silver_parallel': [], 'silver_sequential': []}))
    out = capsys.readouterr().out
    assert 'Sanity check: Validate settings check passed.' in out


def test_initialize_empty_tables_dependency_order(monkeypatch):
    class DummyStructType:
        @staticmethod
        def fromJson(js):
            return js

    monkeypatch.setattr(sanity, 'StructType', DummyStructType)

    bronze_path = 'bronze.json'
    silver_path = 'silver.json'
    monkeypatch.setattr(
        sanity,
        '_discover_settings_files',
        lambda: ({'b': bronze_path}, {'s': silver_path}, {}, {}),
    )

    import builtins, io, json

    def fake_open(p, *a, **k):
        if p == bronze_path:
            return io.StringIO(
                json.dumps(
                    {
                        'dst_table_name': 'cat.bronze.b',
                        'file_schema': {'type': 'struct', 'fields': []},
                        'transform_function': 't.bronze',
                    }
                )
            )
        if p == silver_path:
            return io.StringIO(
                json.dumps(
                    {
                        'src_table_name': 'cat.bronze.b',
                        'dst_table_name': 'cat.silver.s',
                        'transform_function': 't.silver',
                    }
                )
            )
        return builtins.open(p, *a, **k)

    monkeypatch.setattr(builtins, 'open', fake_open)

    created = []

    def fake_create_table(df, dst, spark):
        created.append(dst)
        spark.tables[dst] = df

    def fake_get_function(name):
        return lambda df, settings, spark: df

    class DummyDF(dict):
        def limit(self, n):  # pragma: no cover - simple passthrough
            return self

    class DummySpark:
        def __init__(self):
            self.tables = {}

        def createDataFrame(self, data, schema):
            return DummyDF(schema=schema)

        def table(self, name):
            return self.tables[name]

    spark = DummySpark()
    monkeypatch.setattr(sanity, 'create_table_if_not_exists', fake_create_table)
    monkeypatch.setattr(sanity, 'get_function', fake_get_function)

    sanity.initialize_empty_tables(spark)
    assert created == ['cat.bronze.b', 'cat.silver.s']


def test_initialize_empty_tables_external_source(monkeypatch):
    silver_path = 'silver.json'
    monkeypatch.setattr(
        sanity,
        '_discover_settings_files',
        lambda: ({}, {'s': silver_path}, {}, {}),
    )

    import builtins, io, json

    def fake_open(p, *a, **k):
        if p == silver_path:
            return io.StringIO(
                json.dumps(
                    {
                        'src_table_name': 'cat.bronze.b',
                        'dst_table_name': 'cat.silver.s',
                        'transform_function': 't.silver',
                    }
                )
            )
        return builtins.open(p, *a, **k)

    monkeypatch.setattr(builtins, 'open', fake_open)

    created = []

    def fake_create_table(df, dst, spark):
        created.append(dst)

    def fake_get_function(name):
        return lambda df, settings, spark: df

    class DummyDF(dict):
        def limit(self, n):
            return self

    class DummySpark:
        def __init__(self):
            self.tables = {'cat.bronze.b': DummyDF()}

        def table(self, name):
            return self.tables[name]

    spark = DummySpark()
    monkeypatch.setattr(sanity, 'create_table_if_not_exists', fake_create_table)
    monkeypatch.setattr(sanity, 'get_function', fake_get_function)

    sanity.initialize_empty_tables(spark)
    assert created == ['cat.silver.s']
