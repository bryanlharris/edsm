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

    monkeypatch.setattr(sanity, '_discover_settings_files', lambda: ({}, {}, {}))
    sanity.validate_settings(DummyDbutils())

    out = capsys.readouterr().out
    assert 'Sanity check: Validate settings check passed.' in out
    assert 'WARNING' in out
    assert config.S3_ROOT_LANDING.endswith('/')
    assert config.S3_ROOT_UTILITY.endswith('/')
