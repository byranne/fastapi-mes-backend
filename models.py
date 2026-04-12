from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from sqlalchemy import UniqueConstraint
from sqlmodel import Field, SQLModel


class ProcessEvent(SQLModel, table=True):
    __tablename__ = "process_event"
    __table_args__ = (
        UniqueConstraint("unit_id", "step_id", name="uq_process_event_unit_step"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    event_id: str
    unit_id: str
    station_id: str
    step_id: str
    occurred_at: datetime


class EventCreate(BaseModel):
    event_id: str
    unit_id: str
    station_id: str
    step_id: str
    occurred_at: datetime
