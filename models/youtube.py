from pydantic import BaseModel, Field
from datetime import datetime, date
from typing import Optional, Union

class Channel(BaseModel):
    id: str
    handle: str
    title: str
    description: str
    custom_url: str
    published_at: datetime
    thumbnail_url: str
    view_count: int
    subscriber_count: int
    hidden_subscriber_count: bool
    video_count: int
    uploads_playlist_id: str
    ingested_at: datetime
    
class UploadPlaylist(BaseModel):
    channel_id: str
    channel_handle: str
    uploads_playlist_id: str
    
class Video(BaseModel):
    title: str
    url: str
    
# Metadata schema
class VideoMetadata(BaseModel):
    """
    Structured representation of yt-dlp data for video.
    """
    video_id: str
    title: str
    channel: str
    channel_id: str
    duration_seconds: int
    view_count: int
    like_count: Optional[int]
    comment_count: Optional[int]
    upload_date: Union[date, str] 
    description: str
    tags: list[str] = Field(default_factory=list)
    webpage_url: str = ""
    ext: str = "mp4"
    storage_uri: str = "" # After saving
    ingested_at: Optional[datetime] = None
    frames_processed_at: Optional[datetime] = None # After embedding frames
    transcript_exists: Optional[bool] = None