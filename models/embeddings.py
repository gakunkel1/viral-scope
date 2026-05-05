from pydantic import BaseModel, Field
from datetime import datetime, UTC

class TranscriptSegment(BaseModel):
    start: float
    end: float
    text: str
    
class FrameInfo(BaseModel):
    timestamp_sec: float
    path: str
    categories: dict[str, float] = Field(default_facory=dict)
    
class TranscriptChunk(BaseModel):
    id: str
    text: str
    start_time: float
    end_time: float
    segment_ids: list[int]
    video_id: str
    