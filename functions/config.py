from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def get_catalog(dbutils: Any) -> str:
    """Return the workspace catalog widget value."""
    return dbutils.widgets.get("catalog")


def get_settings_s3_prefix(dbutils: Any) -> str:
    """Return the S3 prefix widget value used for settings."""
    return dbutils.widgets.get("settings_s3_prefix")
