from pydantic import BaseModel, Field
from datetime import datetime

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