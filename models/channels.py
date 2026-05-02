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