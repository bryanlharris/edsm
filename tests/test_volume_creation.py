import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from functions import utility, config

class DummySpark:
    def __init__(self):
        self.queries = []
        self.schemas = set()
        self.volumes = set()
        self.schema_owners = {}
        self.volume_owners = {}

    class _Count:
        def __init__(self, count):
            self._count = count

        def count(self):
            return self._count

    class _Describe:
        def __init__(self, owner):
            self._owner = owner

        def collect(self):
            return [("Owner", self._owner)]

    def sql(self, query):
        self.queries.append(query)

        if query.startswith("SHOW SCHEMAS IN"):
            catalog = query.split()[3]
            schema = query.split()[-1].strip("'")
            count = 1 if (catalog, schema) in self.schemas else 0
            return self._Count(count)

        if query.startswith("SHOW VOLUMES IN"):
            cat_schema = query.split()[3]
            catalog, schema = cat_schema.split(".")
            volume = query.split()[-1].strip("'")
            count = 1 if (catalog, schema, volume) in self.volumes else 0
            return self._Count(count)

        if query.startswith("CREATE SCHEMA"):
            catalog_schema = query.split()[-1]
            catalog, schema = catalog_schema.split(".")
            self.schemas.add((catalog, schema))
            self.schema_owners[(catalog, schema)] = self.schema_owners.get((catalog, schema), "user")

        if query.startswith("CREATE EXTERNAL VOLUME"):
            parts = query.split()
            cat_schema_vol = parts[6]
            catalog, schema, volume = cat_schema_vol.split(".")
            self.volumes.add((catalog, schema, volume))
            self.volume_owners[(catalog, schema, volume)] = self.volume_owners.get((catalog, schema, volume), "user")

        if query.startswith("DESCRIBE SCHEMA EXTENDED"):
            cat_schema = query.split()[3]
            catalog, schema = cat_schema.split(".")
            owner = self.schema_owners.get((catalog, schema), "user")
            return self._Describe(owner)

        if query.startswith("DESCRIBE VOLUME EXTENDED"):
            cat_schema_vol = query.split()[3]
            catalog, schema, volume = cat_schema_vol.split(".")
            owner = self.volume_owners.get((catalog, schema, volume), "user")
            return self._Describe(owner)

        if query.startswith("ALTER SCHEMA") and "OWNER TO" in query:
            cat_schema = query.split()[2]
            catalog, schema = cat_schema.split(".")
            new_owner = query.split()[-1].strip("`")
            self.schema_owners[(catalog, schema)] = new_owner

        if query.startswith("ALTER VOLUME") and "OWNER TO" in query:
            cat_schema_vol = query.split()[2]
            catalog, schema, volume = cat_schema_vol.split(".")
            new_owner = query.split()[-1].strip("`")
            self.volume_owners[(catalog, schema, volume)] = new_owner

        return None


def test_create_landing_volume_uses_landing_root():
    spark = DummySpark()
    utility.create_volume_if_not_exists('cat', 'sch', 'landing', spark)
    assert (
        f"CREATE EXTERNAL VOLUME IF NOT EXISTS cat.sch.landing LOCATION '{config.S3_ROOT_LANDING}cat/sch/landing'" in spark.queries
    )

def test_create_utility_volume_uses_utility_root():
    spark = DummySpark()
    utility.create_volume_if_not_exists('cat', 'sch', 'utility', spark)
    assert (
        f"CREATE EXTERNAL VOLUME IF NOT EXISTS cat.sch.utility LOCATION '{config.S3_ROOT_UTILITY}cat/sch/utility'" in spark.queries
    )



def test_create_volume_message_depends_on_existence(capsys):
    spark = DummySpark()
    # Volume doesn't exist - should print message
    utility.create_volume_if_not_exists('cat', 'sch', 'landing', spark)
    out = capsys.readouterr().out
    assert "Volume did not exist and was created" in out
    assert f"Owner changed to {config.OBJECT_OWNER}" in out

    # Mark volume as existing and call again - no message expected
    spark.volumes.add(('cat', 'sch', 'landing'))
    utility.create_volume_if_not_exists('cat', 'sch', 'landing', spark)
    out = capsys.readouterr().out
    assert out == ""


def test_create_schema_message_depends_on_existence(capsys):
    spark = DummySpark()
    # Schema not present
    utility.create_schema_if_not_exists('cat', 'sch', spark)
    out = capsys.readouterr().out
    assert "Schema did not exist and was created" in out
    assert f"Owner changed to {config.OBJECT_OWNER}" in out

    # Already present
    spark.schemas.add(('cat', 'sch'))
    utility.create_schema_if_not_exists('cat', 'sch', spark)
    out = capsys.readouterr().out
    assert out == ""

def test_volume_root_without_trailing_slash(monkeypatch):
    spark = DummySpark()
    root_no_slash = config.S3_ROOT_LANDING.rstrip('/')
    monkeypatch.setattr(config, 'S3_ROOT_LANDING', root_no_slash)
    utility.create_volume_if_not_exists('cat', 'sch', 'landing', spark)
    assert (
        f"CREATE EXTERNAL VOLUME IF NOT EXISTS cat.sch.landing LOCATION '{root_no_slash}/cat/sch/landing'" in spark.queries
    )


def test_owner_alter_queries_executed():
    spark = DummySpark()
    utility.create_schema_if_not_exists('cat', 'sch', spark)
    assert any(q.startswith(f'ALTER SCHEMA cat.sch OWNER TO `{config.OBJECT_OWNER}`') for q in spark.queries)

    spark = DummySpark()
    utility.create_volume_if_not_exists('cat', 'sch', 'utility', spark)
    assert any(q.startswith(f'ALTER VOLUME cat.sch.utility OWNER TO `{config.OBJECT_OWNER}`') for q in spark.queries)

