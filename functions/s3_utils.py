from urllib.parse import urlparse
from typing import Optional
import boto3

_session: Optional[boto3.session.Session] = None


def _parse_s3_uri(uri: str):
    """Return bucket and key from an S3 URI."""
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Invalid S3 URI: {uri}")
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    return bucket, key


def _get_session(dbutils=None) -> boto3.session.Session:
    """Return a boto3 session, optionally using credentials from ``dbutils``."""
    global _session
    if _session is None:
        if dbutils is not None:
            access_key = dbutils.secrets.get(scope="myscope", key="aws_access_key_id")
            secret_key = dbutils.secrets.get(scope="myscope", key="aws_secret_access_key")
            _session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name="us-east-1",
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
