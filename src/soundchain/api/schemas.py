from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field

class ListenEventSchema(BaseModel):
    
    event_id: str = Field(..., description="Unique event ID (UUID from client)")
    user_id: UUID
    track_id: UUID
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    duration_played_ms: int = Field(..., gt=0, description="How long user listened")
    
    country: str = Field(default="US", min_length=2, max_length=2)
    platform: str = Field(default="web")

    class Config:
        populate_by_name = True