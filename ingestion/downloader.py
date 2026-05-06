import logging
from pydantic import BaseModel, Field
from pathlib import Path
from typing import Optional
from datetime import datetime

import yt_dlp

from ingestion.storage import StorageBackend, get_storage_backend
from models.youtube import VideoMetadata

logger = logging.getLogger(__name__)

# Request best video at 720p or below + best m4a audio, then best single file mp4, then "best"
# mp4 and 720p offers greatest compatibility and reduces processing overhead
FORMAT_STRING = (
    "bestvideo[ext=mp4][vcodec^=avc1][height<=720]"
    "+bestaudio[ext=m4a]"
    "/best[ext=mp4][height<=720]"
    "/best"
)

# Default settings for yt-dlp
BASE_YDL_OPTS: dict = {
    "format": FORMAT_STRING,
    "merge_output_format": "mp4", # for FFmpeg
    "js_runtimes": {"deno": {}},
    
    # Metadata
    "writeinfojson": True,
    
    # Rate limiting, etc
    "ratelimit": 2_000_000,
    "sleep_interval": 2,
    "max_sleep_interval": 6,
    "retries": 4,
    "fragment_retries": 4,
    "file_access_retries": 3,
    
    "nooverwrites": True,
    "quiet": True,
    "no_warnings": False
}

class VideoDownloader:
    """
    Downloader for YouTube videos with idempotency using storage backend.
    """
    
    def __init__(
        self,
        storage: Optional[StorageBackend] = None,
        tmp_dir: str = "data/tmp",
    ):
        self.storage = storage or get_storage_backend()
        self.tmp_dir = Path(tmp_dir)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        
    def download(self, url: str) -> tuple[str, VideoMetadata]:
        """
        Download single video.
        
        Returns:
            (storage_uri, metadata): storage_uri can be local path or s3:// URI
            
        If video exists in storage, skip download and return (safe for rerun).
        """
        info = self._extract_info(url)
        video_id = info["id"]
        ext = "mp4"

        if self.storage.exists(video_id, ext):
            logger.info(f"[{video_id}] already exists in storage, so skipping download.")
            uri = self.storage.get_uri(video_id, ext)
            metadata = self._parse_metadata(info, uri)
            return uri, metadata
        
        uri = self._download_and_store(url, video_id, ext)
        metadata = self._parse_metadata(info, uri)
        logger.info(f"[{video_id}] saved to {uri}")
        return uri, metadata
    
    def download_channel(
        self, channel_url: str, max_videos: int = 20
    ) -> list[tuple[str, VideoMetadata]]:
        """
        Download N most recent videos from a channel or playlist URL.
        """
        opts = {
            **BASE_YDL_OPTS,
            "outtmpl": str(self.tmp_dir / "%(id)s.%(ext)s"),
            "playlistend": max_videos
        }
        
        results = []
        
        with yt_dlp.YoutubeDL(opts) as ydl:
            playlist_info = ydl.extract_info(channel_url, download=True)
            
        for entry in playlist_info.get("entries") or []:
            if entry is None:
                continue
            video_id = entry["id"]
            ext = "mp4"
            tmp_path = self.tmp_dir / f"{video_id}.{ext}"
            
            if not tmp_path.exists():
                logger.warning(f"[{video_id}] expected file not found at {tmp_path}")
                continue
            
            uri = self.storage.save(tmp_path, video_id, ext)
            metadata = self._parse_metadata(entry, uri)
            results.append((uri, metadata))
            
        return results
    
    def extract_metadata_only(self, url: str) -> VideoMetadata:
        """
        Retrieve metadata without downloading a video file.
        """
        info = self._extract_info(url)
        return self._parse_metadata(info, storage_uri="")
    
    def _extract_info(self, url: str) -> dict:
        """Fetch video metadata from YouTube without downloading."""
        with yt_dlp.YoutubeDL({"quiet": True, "no_warnings": True}) as ydl:
            return ydl.extract_info(url, download=False)
        
    def _download_and_store(self, url: str, video_id: str, ext: str) -> str:
        """Download to tmp, then pass to storage backend."""
        opts = {
            **BASE_YDL_OPTS,
            "outtmpl": str(self.tmp_dir / f"{video_id}.%(ext)s")
        }
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([url])
            
        tmp_path = self.tmp_dir / f"{video_id}.{ext}"
        return self.storage.save(tmp_path, video_id, ext)
    
    def _parse_metadata(self, info: dict, storage_uri: str) -> VideoMetadata:
        """
        Translate yt-dlp info dict into VideoMetadata schema.
        """
        return VideoMetadata(
            video_id=info["id"],
            title=info.get("title", ""),
            channel=info.get("channel") or info.get("uploader", ""),
            channel_id=info.get("channel_id") or info.get("uploader_id", ""),
            duration_seconds=info.get("duration") or 0,
            view_count=info.get("view_count") or 0,
            like_count=info.get("like_count"),
            comment_count=info.get("comment_count"),
            upload_date=info.get("upload_date", ""),
            description=info.get("description", ""),
            tags=info.get("tags") or [],
            webpage_url=info.get("webpage_url", ""),
            ext="mp4",
            storage_uri=storage_uri,
        )