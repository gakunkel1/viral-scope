import os
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from contextlib import contextmanager
import tempfile

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class StorageBackend(ABC):
    """
    Abstract base class for video storage.
    
    New backends need to implement these methods.
    """
    
    @abstractmethod
    def save(self, tmp_path: Path, video_id: str, ext: str) -> str:
        """
        Move/upload file at tmp_path to permanent storage.
        Returns URI string (file path or s3://... URI).
        Implementation must clean up tmp_path if needed.
        """
        
    @abstractmethod
    def exists(self, video_id: str, ext: str) -> bool:
        """Return True if the video already exists in storage for idempotency."""
        
    @abstractmethod
    def get_uri(self, video_id: str, ext: str) -> str:
        """Return the URI for an existing video from storage without downloading."""
        
    @contextmanager
    @abstractmethod
    def scoped_local_path(self, video_id: str, ext: str):
        yield str(self._path(video_id, ext))
        

class LocalStorage(StorageBackend):
    """
    Stores video as plain files on local filesystem.
    """
    
    def __init__(self, base_dir: str = "data/videos"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"LocalStorage initialized at {self.base_dir.resolve()}")
        
    @contextmanager
    def scoped_local_path(self, video_id: str, ext: str):
        yield str(self._path(video_id, ext))
        
    def _path(self, video_id: str, ext: str) -> Path:
        return self.base_dir / f"{video_id}.{ext}"
    
    def save(self, tmp_path: Path, video_id: str, ext: str) -> str:
        dest = self._path(video_id, ext)
        if tmp_path.resolve() != dest.resolve():
            tmp_path.rename(dest)
        return str(dest)
    
    def exists(self, video_id: str, ext: str) -> bool:
        return self._path(video_id, ext).exists()
    
    def get_uri(self, video_id: str, ext: str) -> str:
        return str(self._path(video_id, ext))
    
    
class S3Storage(StorageBackend):
    """
    Stores videos in S3 bucket.
    
    Use boto3 managed multipart uploader (upload_file) to split
    large files into parts and upload in parallel.
    
    Auth: Uses standard AWS credential chain:
        1. Env vars (AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY)
        2. ~/.aws/credentials file
        3. IAM instance role (preferred for production on EC2/ECS)
    """
    
    def __init__(self, bucket: str, prefix: str = "videos/"):
        self.bucket = bucket
        # Normalize prefix
        self.prefix = prefix.rstrip("/") + "/"
        self.client = boto3.client("s3")
        logger.info(f"S3Storage initialized: s3://{self.bucket}/{self.prefix}")
        
    @contextmanager
    def scoped_local_path(self, video_id, ext):
        key = self._key(video_id, ext)
        # Create a temp file that is deleted when the context exits
        with tempfile.NamedTemporaryFile(suffix=f".{ext}") as tmp:
            self.client.download_file(self.bucket, key, tmp.name)
            yield tmp.name
        
    def _key(self, video_id: str, ext: str) -> str:
        return f"{self.prefix}{video_id}.{ext}"

    def save(self, tmp_path: Path, video_id: str, ext: str) -> str:
        key = self._key(video_id, ext)
        logger.info(f"Uploading {tmp_path} to s3://{self.bucket}/{key}")
        self.client.upload_file(
            Filename=str(tmp_path),
            Bucket=self.bucket,
            Key=key,
            ExtraArgs={"ContentType": f"video/{ext}"}
        )
        # Remove local temp file
        tmp_path.unlink()
        return f"s3://{self.bucket}/{key}"
    
    def exists(self, video_id: str, ext: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(video_id, ext))
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise
        
    def get_uri(self, video_id: str, ext: str) -> str:
        return f"s3://{self.bucket}/{self._key(video_id, ext)}"
    
    
def get_storage_backend() -> StorageBackend:
    """
    Factory function that reads LOCAL_VIDEO_STORAGE and returns correct backend.
    """
    use_local = os.getenv("LOCAL_VIDEO_STORAGE", "true").lower() in ("true", "1", "yes", "y")
    
    if use_local:
        base_dir = os.getenv("LOCAL_VIDEO_DIR", "data/videos")
        return LocalStorage(base_dir=base_dir)
    
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        raise EnvironmentError(
            "S3_BUCKET must be set when LOCAL_VIDEO_STORAGE=false"
        )
    prefix = os.getenv("S3_PREFIX", "videos/")
    return S3Storage(bucket=bucket, prefix=prefix)