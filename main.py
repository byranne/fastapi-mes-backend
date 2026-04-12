import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, status
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, SQLModel, create_engine

from models import EventCreate, ProcessEvent

DATABASE_URL = "sqlite:///./app.db"

# check_same_thread=False allows SQLAlchemy sessions across FastAPI worker threads.
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

logger = logging.getLogger("process-events")


@asynccontextmanager
async def lifespan(_: FastAPI):
    SQLModel.metadata.create_all(engine)
    yield


app = FastAPI(lifespan=lifespan)


def get_session():
    with Session(engine) as session:
        yield session


@app.post("/api/events", status_code=status.HTTP_200_OK)
def create_event(payload: EventCreate, session: Session = Depends(get_session)):
    event = ProcessEvent(**payload.model_dump())
    session.add(event)

    try:
        session.commit()
        return {"status": "saved"}
    except IntegrityError:
        session.rollback()
        logger.info(
            "Duplicate process event ignored",
            extra={"unit_id": payload.unit_id, "step_id": payload.step_id},
        )
        # Return 200 so stations stop retrying duplicate deliveries.
        return {"status": "duplicate_ignored"}
