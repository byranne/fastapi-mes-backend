# Peak Energy MES Backend Take-Home

## Overview

This project is a data-driven MES manufacturing line backend system.

Simulated manufactoring stations publish events to backend which stores them in a local database. Handles retries, duplicates, and out-of-order entry arrival.

This implemenation is particularly focused in handling:
- duplicate events under concurrency
- preserving accepted events across system restarts
- maintaining event progression tracking even when events arrive out-of-order

## Problem Context

We have an existing ESS manufacturing line, which under normal conditions works fine. 

Design a backend system that works under abnormal conditions as follows:

- Events from 10 stations arrive concurrently and out of order
- The network between stations and the backend is unreliable — events may be delayed, duplicated, or lost
- Stations buffer locally and retry on failure, but provide no exactly-once delivery guarantee
- The backend must never write a duplicate record for the same process step on the same unit
- A station may restart mid-production and re-publish events for any step it believes was not acknowledged
- The backend must remain eventually consistent across crashes and restarts
- Two stations may attempt to update the same unit record simultaneously

## Design Summary

### Core data model

`ProcessEvent` (`models.py`) establishes `(unit_id, step_id)` must be unique.

Key columns:

- `event_id`: original station event identifier
- `unit_id`: production unit identifier
- `station_id`: station source
- `step_id`: process step
- `occurred_at`: station event timestamp
- `step_index`: normalized index of configured step sequence
- `unit_state`: computed state snapshot at ingestion time

### Idempotency strategy

Event table enforces a uniqueness constraint:

- `UNIQUE(unit_id, step_id)`

The API perfroms a database lookup on incoming request to ensure on duplicate entries and handles race conditions by catching 'IntegrityError' on database commit.

This approach prevents identical events from clogging up the database and deals with concurrent collision conflicts.

### Event ordering strategy

Events are accepted even if out of order.

The backend computes state using the set of already-known steps for a unit plus the incoming step:

- if all steps present -> `COMPLETE`
- if no contiguous step from the beginning -> `AT_START`
- otherwise -> `AT_<latest_contiguous_step>`

This supports delayed delivery and buffered replay without rejecting valid late events.

### Durability and restart behavior

- SQLite file (`app.db`) provides durable local storage.
- On startup, schema creation runs via SQLModel metadata.

## Architecture Diagram

![Event Processing Architecture](./doc-assets/Model.svg)

## API

### `POST /api/events`

Request body:

```json
{
	"event_id": "4c4f2fbe-8f08-4cbe-9e8a-2e51452ad3f0",
	"unit_id": "UNIT-0001",
	"station_id": "station-01",
	"step_id": "STEP-ALPHA",
	"occurred_at": "2026-04-13T12:00:00Z"
}
```

Responses:

- `200 {"status": "saved"}` when a new logical unit-step is persisted
- `200 {"status": "duplicate_ignored"}` when replay/duplicate is detected
- `400 {"status": "invalid_step", "known_steps": [...]}` for unknown step IDs

## Local Setup

1. Create and activate a virtual environment.
2. Install dependencies.
3. Run the API server.

Example:

```bash
python -m venv .venv
source .venv/bin/activate
pip install fastapi uvicorn sqlmodel sqlalchemy httpx pytest pytest-asyncio
uvicorn main:app --reload
```

The service runs at `http://127.0.0.1:8000`.

## Running the Simulation

The simulator fires concurrent events from 10 stations for one unit/step and verifies idempotency:

```bash
python simulate_stations.py
```

Expected outcome:

- one event reported as `saved`
- remaining duplicates reported as `duplicate_ignored`
- DB row count for that unit/step equals `1`

## Tests

Run tests:

```bash
pytest -v
```

Covered scenarios (`test_simulate_stations.py`):

- sequential happy path across all three steps
- concurrent duplicate events for same unit step
- out-of-order delivery
- delayed replay from local station buffer
- concurrent different steps for the same unit

## Conclusions

### explicit trade-offs

## Hard Problems and Decisions

### Hardest problems


### Chosen approach


### Explicitly not solved in this scope


## Future Improvements


## Project Files

- `main.py`: FastAPI app and ingestion logic
- `models.py`: SQLModel table and request schema
- `simulate_stations.py`: standard concurrency test
- `test_simulate_stations.py`: async test scenarios
- `app.db`: local SQLite database file
- `doc-assets`: diagrams/models used to render in `README.md`