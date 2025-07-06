from urllib.parse import urlparse
from typing import Optional
import boto3
import os

_session: Optional[boto3.session.Session] = None


def _parse_s3_uri(uri: str):
    """Return bucket and key from an S3 URI."""
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Invalid S3 URI: {uri}")
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    return bucket, key


def _get_session(dbutils=None, spark=None) -> boto3.session.Session:
    """Return a boto3 session using available credentials.

    When ``dbutils`` is provided, AWS keys are fetched from Databricks secrets
    and stored in ``os.environ`` so that executors launched via ``addPyFile`` can
    authenticate.  ``spark`` is accepted for backward compatibility but is not
    used.
    """

    global _session
    if _session is None:
        access_key = None
        secret_key = None

        if dbutils is not None:
            access_key = dbutils.secrets.get(scope="myscope", key="aws_access_key_id")
            secret_key = dbutils.secrets.get(scope="myscope", key="aws_secret_access_key")
            os.environ["AWS_ACCESS_KEY_ID"] = access_key
            os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key
        else:
            access_key = os.environ.get("AWS_ACCESS_KEY_ID")
            secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

        if access_key and secret_key:
            _session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name="us-west-2",
            )
        else:
            _session = boto3.Session()

    return _session


def upload_file(local_path: str, s3_uri: str, dbutils=None):
    """Upload a local file to ``s3_uri``."""
    bucket, key = _parse_s3_uri(s3_uri)
    client = _get_session(dbutils).client("s3")
    client.upload_file(local_path, bucket, key)


def download_file(s3_uri: str, local_path: str, dbutils=None):
    """Download ``s3_uri`` to ``local_path``."""
    bucket, key = _parse_s3_uri(s3_uri)
    client = _get_session(dbutils).client("s3")
    client.download_file(bucket, key, local_path)
