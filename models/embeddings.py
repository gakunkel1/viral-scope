from pydantic import BaseModel, Field
from datetime import datetime

class TranscriptSegment(BaseModel):
    start: float
    end: float
    text: str
    
class FrameInfo(BaseModel):
    timestamp_sec: float
    path: str
    categories: dict[str, float] = Field(default_facory=dict)
    
class CreatorVideo(BaseModel):
    video_id: str
    url: str
    title: str = ""
    channel: str = ""
    channel_id: str = ""
    duration_sec: float = 0.0
    upload_date: str = ""
    description: str = ""
    view_count: int = 0
    video_path: str = ""
    audio_path: str = ""
    frame_paths: list[str] = Field(default_factory=list)
    transcript: list[TranscriptSegment] = Field(default_factory=list)
    frames: list[FrameInfo] = Field(default_factory=list)
    ingested_at: datetime = Field(default_factory=datetime.now(datetime.timezone.utc))