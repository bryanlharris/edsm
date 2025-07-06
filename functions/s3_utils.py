from urllib.parse import urlparse
import boto3

def _parse_s3_uri(uri: str):
    """Return bucket and key from an S3 URI."""
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Invalid S3 URI: {uri}")
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    return bucket, key


def upload_file(local_path: str, s3_uri: str):
    """Upload a local file to ``s3_uri``."""
    bucket, key = _parse_s3_uri(s3_uri)
    client = boto3.client("s3")
    client.upload_file(local_path, bucket, key)


def download_file(s3_uri: str, local_path: str):
    """Download ``s3_uri`` to ``local_path``."""
    bucket, key = _parse_s3_uri(s3_uri)
    client = boto3.client("s3")
    client.download_file(bucket, key, local_path)
