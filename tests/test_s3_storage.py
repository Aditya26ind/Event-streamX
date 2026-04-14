from pathlib import Path
import sys
from io import BytesIO

from botocore.exceptions import ClientError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.s3_storage import S3Storage


class FakeS3Client:
    def __init__(self, should_create_bucket: bool = False) -> None:
        self.should_create_bucket = should_create_bucket
        self.head_bucket_calls: list[str] = []
        self.created_buckets: list[dict] = []
        self.uploads: list[dict] = []
        self.puts: list[dict] = []
        self.objects: dict[str, bytes] = {}

    def head_bucket(self, Bucket: str) -> None:
        self.head_bucket_calls.append(Bucket)
        if self.should_create_bucket:
            raise ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}},
                "HeadBucket",
            )

    def create_bucket(self, **kwargs) -> None:
        self.created_buckets.append(kwargs)

    def upload_file(self, filename: str, bucket: str, key: str, ExtraArgs: dict) -> None:
        self.uploads.append(
            {
                "filename": filename,
                "bucket": bucket,
                "key": key,
                "extra_args": ExtraArgs,
            }
        )

    def put_object(self, **kwargs) -> None:
        self.puts.append(kwargs)
        self.objects[kwargs["Key"]] = kwargs["Body"]

    def get_object(self, Bucket: str, Key: str) -> dict:
        return {"Body": BytesIO(self.objects[Key])}

    def get_paginator(self, _name: str):
        client = self

        class Paginator:
            def paginate(self, Bucket: str, Prefix: str):
                contents = [
                    {"Key": key}
                    for key in sorted(client.objects)
                    if key.startswith(Prefix)
                ]
                yield {"Contents": contents}

        return Paginator()


def test_upload_file_skips_when_disabled(monkeypatch, tmp_path: Path) -> None:
    file_path = tmp_path / "events.jsonl"
    file_path.write_text("{\"event_id\":\"1\"}\n", encoding="utf-8")
    monkeypatch.delenv("S3_STORAGE_ENABLED", raising=False)

    storage = S3Storage()

    assert storage.upload_file(file_path) is False


def test_upload_file_creates_bucket_and_uploads(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("S3_STORAGE_ENABLED", "true")
    monkeypatch.setenv("S3_BUCKET_NAME", "eventstreamx-data")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    file_path = tmp_path / "data" / "raw" / "events" / "sample.jsonl"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text("{\"event_id\":\"1\"}\n", encoding="utf-8")

    storage = S3Storage()
    fake_client = FakeS3Client(should_create_bucket=True)

    monkeypatch.setattr(storage, "_create_client", lambda: fake_client)
    monkeypatch.setattr("scripts.s3_storage.PROJECT_ROOT", tmp_path)

    assert storage.upload_file(file_path) is True
    assert fake_client.created_buckets == [{"Bucket": "eventstreamx-data"}]
    assert fake_client.uploads == [
        {
            "filename": str(file_path.resolve()),
            "bucket": "eventstreamx-data",
            "key": "data/raw/events/sample.jsonl",
            "extra_args": {"ContentType": "application/json"},
        }
    ]


def test_upload_bytes_and_list_keys(monkeypatch) -> None:
    monkeypatch.setenv("S3_STORAGE_ENABLED", "true")

    storage = S3Storage()
    fake_client = FakeS3Client()
    monkeypatch.setattr(storage, "_create_client", lambda: fake_client)

    assert storage.upload_bytes(b"payload", "data/processed/events/sample.parquet") is True
    assert storage.read_file("data/processed/events/sample.parquet") == b"payload"
    assert storage.list_keys("data/processed/events") == [
        "data/processed/events/sample.parquet"
    ]
