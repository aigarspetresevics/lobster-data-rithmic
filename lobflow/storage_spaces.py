from pathlib import Path
import boto3

class SpacesUploader:
    def __init__(self, *, endpoint: str, region: str, bucket: str):
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            region_name=region,
            endpoint_url=endpoint,
            aws_access_key_id=None,         # pulled from env by boto3
            aws_secret_access_key=None,     # pulled from env by boto3
        )

    def upload_file(self, local_path: Path, key: str) -> int:
        local_path = Path(local_path)
        self.client.upload_file(str(local_path), self.bucket, key)
        return local_path.stat().st_size
