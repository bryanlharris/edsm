import sys
import types
import pathlib

# Stub minimal pyspark modules so functions can be imported without pyspark
pyspark = types.ModuleType('pyspark')
sql = types.ModuleType('pyspark.sql')
types_mod = types.ModuleType('pyspark.sql.types')
sql.types = types_mod
pyspark.sql = sql
sys.modules.setdefault('pyspark', pyspark)
sys.modules.setdefault('pyspark.sql', sql)
sys.modules.setdefault('pyspark.sql.types', types_mod)
types_mod.StructType = type('StructType', (), {})

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
            @staticmethod
            def get(taskKey=None, key=None):
                return None

        def __init__(self):
            self.taskValues = self.TaskValues()

    def __init__(self):
        self.jobs = self.Jobs()


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


def test_warn_history_dst_table_names(capsys, monkeypatch):
    path = pathlib.Path('dummy.json')
    dummy_root = types.SimpleNamespace(glob=lambda pattern: [path] if 'bronze_history' in pattern else [])
    monkeypatch.setattr(sanity, 'PROJECT_ROOT', dummy_root)

    import builtins, io, json

    def fake_open(p, *a, **k):
        if p == path:
            return io.StringIO(json.dumps({'full_table_name': 'cat.sch.tbl'}))
        return builtins.open(p, *a, **k)

    monkeypatch.setattr(builtins, 'open', fake_open)

    sanity.warn_history_dst_table_names()

    out = capsys.readouterr().out
    assert 'dummy.json' in out
    assert 'cat.sch.tbl_history' in out


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
