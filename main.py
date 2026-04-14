import logging
import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, status
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, SQLModel, create_engine, select

from models import EventCreate, ProcessEvent

DATABASE_URL = "sqlite:///./app.db"

# check_same_thread=False allows S  QLAlchemy sessions across FastAPI worker threads.
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

logger = logging.getLogger("process-events")

STEP_SEQUENCE = [
    step.strip()
    for step in os.getenv("STEP_SEQUENCE", "STEP-ALPHA,STEP-BETA,STEP-GAMMA").split(",")
    if step.strip()
]
STEP_INDEX_BY_ID = {step_id: index for index, step_id in enumerate(STEP_SEQUENCE)}

#input step_index output current step/machine
def _state_for_step_index(step_index: int) -> str:
    if step_index == len(STEP_SEQUENCE) - 1:
        return "COMPLETE"
    return f"AT_{STEP_SEQUENCE[step_index]}"

#input current SQL session and unit_id and output collects all step id values into a set
def _existing_step_ids(session: Session, unit_id: str) -> set[str]:
    query = select(ProcessEvent.step_id).where(ProcessEvent.unit_id == unit_id)
    return set(session.exec(query).all())

#input step a set of all step id's and return 
def _state_for_unit_steps(step_ids: set[str]) -> str:
    if all(step_id in step_ids for step_id in STEP_SEQUENCE):
        return "COMPLETE"

    highest_contiguous_index = -1
    for index, step_id in enumerate(STEP_SEQUENCE):
        if step_id in step_ids:
            highest_contiguous_index = index
            continue
        break

    if highest_contiguous_index < 0:
        return "AT_START"

    return _state_for_step_index(highest_contiguous_index)

#input SQL session, unit_id, incoming_step_id -> computes/returns following step
def _state_after_accepting_step(session: Session, unit_id: str, incoming_step_id: str) -> str:
    step_ids = _existing_step_ids(session, unit_id)
    step_ids.add(incoming_step_id)
    return _state_for_unit_steps(step_ids)

# startup routine
@asynccontextmanager
async def lifespan(_: FastAPI):
    SQLModel.metadata.create_all(engine)
    yield


app = FastAPI(lifespan=lifespan)

# opens connection to SQLite(session)
def get_session():
    with Session(engine) as session:
        yield session

#listens for data coming into "/api/events"
@app.post("/api/events", status_code=status.HTTP_200_OK)
def create_event(payload: EventCreate, session: Session = Depends(get_session)):
    #validates if a certain step_id is valid within STEPSEQUENCE
    step_index = STEP_INDEX_BY_ID.get(payload.step_id)
    if step_index is None:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "status": "invalid_step",
                "known_steps": STEP_SEQUENCE,
            },
        )
    
    # processes incoming entry by calcuating next state
    next_state = _state_after_accepting_step(session, payload.unit_id, payload.step_id)

    event = ProcessEvent(
        **payload.model_dump(),
        step_index=step_index,
        unit_state=next_state,
    )
    session.add(event)

    # Tries to write to database with duplicate validation(IntegrityError) in the event of double write
    try:
        session.commit()
        return {"status": "saved"}
    except IntegrityError:
        session.rollback()
        logger.info(
            "Duplicate process event ignored",
            extra={"unit_id": payload.unit_id, "step_id": payload.step_id},
        )
        return {"status": "duplicate_ignored"}
