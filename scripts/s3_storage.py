import os
import mimetypes
from pathlib import Path

import boto3
from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError, ClientError

from app.logging_config import setup_logger

logger = setup_logger()

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class S3Storage:
    def __init__(self) -> None:
        self.enabled = os.getenv("S3_STORAGE_ENABLED", "false").lower() == "true"
        self.bucket_name = os.getenv("S3_BUCKET_NAME", "eventstreamx-data")
        self.endpoint_url = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
        self.region_name = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        self.access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
        self.secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

    def upload_file(self, file_path: Path) -> bool:
        if not self.enabled:
            return False

        resolved_path = file_path.resolve()
        if not resolved_path.exists():
            logger.warning(
                "Skipping S3 upload for missing file",
                extra={"file_path": str(resolved_path)},
            )
            return False

        object_key = self.object_key_from_path(resolved_path)
        client = self._create_client()

        try:
            self._ensure_bucket(client)
            extra_args = self._build_extra_args(resolved_path)
            client.upload_file(
                str(resolved_path),
                self.bucket_name,
                object_key,
                ExtraArgs=extra_args,
            )
            logger.info(
                "File mirrored to S3",
                extra={
                    "bucket_name": self.bucket_name,
                    "object_key": object_key,
                    "file_path": str(resolved_path),
                    "endpoint_url": self.endpoint_url,
                },
            )
            return True
        except (BotoCoreError, ClientError, ValueError) as exc:
            logger.exception(
                "Failed to mirror file to S3",
                exc_info=exc,
                extra={
                    "bucket_name": self.bucket_name,
                    "object_key": object_key,
                    "file_path": str(resolved_path),
                    "endpoint_url": self.endpoint_url,
                },
            )
            return False

    def object_key_from_path(self, file_path: Path) -> str:
        return file_path.resolve().relative_to(PROJECT_ROOT).as_posix()

    def upload_bytes(self, payload: bytes, object_key: str) -> bool:
        if not self.enabled:
            return False

        client = self._create_client()
        try:
            self._ensure_bucket(client)
            extra_args = self._build_extra_args_from_key(object_key)
            put_kwargs = {
                "Bucket": self.bucket_name,
                "Key": object_key,
                "Body": payload,
            }
            put_kwargs.update(extra_args)
            client.put_object(**put_kwargs)
            logger.info(
                "Bytes written to S3",
                extra={
                    "bucket_name": self.bucket_name,
                    "object_key": object_key,
                    "endpoint_url": self.endpoint_url,
                },
            )
            return True
        except (BotoCoreError, ClientError) as exc:
            logger.exception(
                "Failed to write bytes to S3",
                exc_info=exc,
                extra={
                    "bucket_name": self.bucket_name,
                    "object_key": object_key,
                    "endpoint_url": self.endpoint_url,
                },
            )
            return False

    def _create_client(self) -> BaseClient:
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

    def _ensure_bucket(self, client: BaseClient) -> None:
        try:
            client.head_bucket(Bucket=self.bucket_name)
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code not in {"404", "NoSuchBucket"}:
                raise

            create_bucket_payload = {"Bucket": self.bucket_name}
            if self.region_name != "us-east-1":
                create_bucket_payload["CreateBucketConfiguration"] = {
                    "LocationConstraint": self.region_name
                }
            client.create_bucket(**create_bucket_payload)
            logger.info(
                "Created S3 bucket",
                extra={
                    "bucket_name": self.bucket_name,
                    "endpoint_url": self.endpoint_url,
                },
            )
    
    def read_file(self, object_key: str) -> bytes:
        if not self.enabled:
            raise RuntimeError("S3 storage is disabled")

        client = self._create_client()
        try:
            response = client.get_object(Bucket=self.bucket_name, Key=object_key)
            return response["Body"].read()
        except (BotoCoreError, ClientError) as exc:
            logger.exception(
                "Failed to read file from S3",
                exc_info=exc,
                extra={
                    "bucket_name": self.bucket_name,
                    "object_key": object_key,
                    "endpoint_url": self.endpoint_url,
                },
            )
            raise RuntimeError(f"Failed to read file from S3: {object_key}") from exc

    def list_keys(self, prefix: str) -> list[str]:
        if not self.enabled:
            raise RuntimeError("S3 storage is disabled")

        client = self._create_client()
        try:
            paginator = client.get_paginator("list_objects_v2")
            keys: list[str] = []
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                for item in page.get("Contents", []):
                    keys.append(item["Key"])
            return keys
        except (BotoCoreError, ClientError) as exc:
            logger.exception(
                "Failed to list files from S3",
                exc_info=exc,
                extra={
                    "bucket_name": self.bucket_name,
                    "prefix": prefix,
                    "endpoint_url": self.endpoint_url,
                },
            )
            raise RuntimeError(f"Failed to list files from S3: {prefix}") from exc

    def _build_extra_args(self, file_path: Path) -> dict[str, str]:
        return self._build_extra_args_from_key(file_path.name)

    def _build_extra_args_from_key(self, object_key: str) -> dict[str, str]:
        if object_key.endswith(".jsonl"):
            return {"ContentType": "application/json"}
        if object_key.endswith(".parquet"):
            return {"ContentType": "application/octet-stream"}

        content_type, _ = mimetypes.guess_type(object_key)
        if content_type:
            return {"ContentType": content_type}
        return {}
