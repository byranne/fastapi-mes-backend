from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from sqlalchemy import UniqueConstraint
from sqlmodel import Field, SQLModel


class ProcessEvent(SQLModel, table=True):
    __tablename__ = "process_event"
    __table_args__ = (
        #enforces uniqueness on data entry
        UniqueConstraint("unit_id", "step_id", name="uq_process_event_unit_step"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    event_id: str
    unit_id: str
    station_id: str
    step_id: str
    occurred_at: datetime
    step_index: int
    unit_state: str


#inherits from pydantic's BaseModel for data validation.
class EventCreate(BaseModel):
    event_id: str
    unit_id: str
    station_id: str
    step_id: str
    occurred_at: datetime
