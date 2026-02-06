import io
import logging
from datetime import timedelta
from typing import Optional, BinaryIO

from minio import Minio
from minio.error import S3Error

import sys
sys.path.insert(0, '..')
from config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
    MINIO_BUCKET_AUDIO,
    MINIO_BUCKET_LOGS,
)

logger = logging.getLogger(__name__)


class MinIOClient:
    """Singleton MinIO client manager."""
    
    _instance: Optional['MinIOClient'] = None
    _client: Optional[Minio] = None
    
    def __new__(cls) -> 'MinIOClient':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._client is None:
            self._client = Minio(
                MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=MINIO_SECURE
            )
            self._ensure_buckets()
    
    def _ensure_buckets(self) -> None:
        """Create required buckets if they don't exist."""
        buckets = [MINIO_BUCKET_AUDIO, MINIO_BUCKET_LOGS]
        
        for bucket in buckets:
            try:
                if not self._client.bucket_exists(bucket):
                    self._client.make_bucket(bucket)
                    logger.info(f"Created bucket: {bucket}")
                else:
                    logger.debug(f"Bucket already exists: {bucket}")
            except S3Error as e:
                logger.error(f"Error creating bucket {bucket}: {e}")
                raise
    
    @property
    def client(self) -> Minio:
        """Get the MinIO client instance."""
        return self._client


def get_minio_client() -> Minio:
    """Get the MinIO client instance."""
    return MinIOClient().client


def ensure_buckets() -> None:
    """Ensure all required buckets exist."""
    # This is called automatically on client initialization
    MinIOClient()


def upload_file(bucket: str, file_path: str, object_name: str, content_type: str = None) -> bool:
    client = get_minio_client()
    
    try:
        result = client.fput_object(
            bucket,
            object_name,
            file_path,
            content_type=content_type
        )
        logger.info(f"Uploaded {file_path} to {bucket}/{object_name}")
        return True
    except S3Error as e:
        logger.error(f"Error uploading {file_path}: {e}")
        return False


def upload_bytes(bucket: str, data: bytes, object_name: str, content_type: str = "application/octet-stream") -> bool:
    client = get_minio_client()
    
    try:
        data_stream = io.BytesIO(data)
        result = client.put_object(
            bucket,
            object_name,
            data_stream,
            length=len(data),
            content_type=content_type
        )
        logger.info(f"Uploaded bytes to {bucket}/{object_name} ({len(data)} bytes)")
        return True
    except S3Error as e:
        logger.error(f"Error uploading bytes to {object_name}: {e}")
        return False


def upload_stream(bucket: str, stream: BinaryIO, object_name: str, length: int, content_type: str = "application/octet-stream") -> bool:
    client = get_minio_client()
    
    try:
        result = client.put_object(
            bucket,
            object_name,
            stream,
            length=length,
            content_type=content_type
        )
        logger.info(f"Uploaded stream to {bucket}/{object_name}")
        return True
    except S3Error as e:
        logger.error(f"Error uploading stream to {object_name}: {e}")
        return False


def get_presigned_url(bucket: str, object_name: str, expires: timedelta = timedelta(hours=1)) -> Optional[str]:
    client = get_minio_client()
    
    try:
        url = client.presigned_get_object(bucket, object_name, expires=expires)
        return url
    except S3Error as e:
        logger.error(f"Error getting presigned URL for {object_name}: {e}")
        return None


def file_exists(bucket: str, object_name: str) -> bool:
    client = get_minio_client()
    
    try:
        client.stat_object(bucket, object_name)
        return True
    except S3Error as e:
        if e.code == "NoSuchKey":
            return False
        logger.error(f"Error checking if {object_name} exists: {e}")
        return False


def download_file(bucket: str, object_name: str, file_path: str) -> bool:
    client = get_minio_client()
    
    try:
        client.fget_object(bucket, object_name, file_path)
        logger.info(f"Downloaded {bucket}/{object_name} to {file_path}")
        return True
    except S3Error as e:
        logger.error(f"Error downloading {object_name}: {e}")
        return False


def get_object_data(bucket: str, object_name: str) -> Optional[bytes]:
    client = get_minio_client()
    
    try:
        response = client.get_object(bucket, object_name)
        data = response.read()
        response.close()
        response.release_conn()
        return data
    except S3Error as e:
        logger.error(f"Error getting object {object_name}: {e}")
        return None


def list_objects(bucket: str, prefix: str = "") -> list:
    client = get_minio_client()
    
    try:
        objects = client.list_objects(bucket, prefix=prefix)
        return [obj.object_name for obj in objects]
    except S3Error as e:
        logger.error(f"Error listing objects in {bucket}: {e}")
        return []


def delete_object(bucket: str, object_name: str) -> bool:
    client = get_minio_client()
    
    try:
        client.remove_object(bucket, object_name)
        logger.info(f"Deleted {bucket}/{object_name}")
        return True
    except S3Error as e:
        logger.error(f"Error deleting {object_name}: {e}")
        return False
