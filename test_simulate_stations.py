import asyncio
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000")
ENDPOINT = "/api/events"


def count_rows_for_step(unit_id: str, step_id: str) -> int:
    """Count rows matching (unit_id, step_id) in the database."""
    db_path = Path(__file__).resolve().parent / "app.db"
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM process_event WHERE unit_id = ? AND step_id = ?",
            (unit_id, step_id),
        )
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def count_rows_for_unit(unit_id: str) -> int:
    """Count all rows for a given unit_id in the database."""
    db_path = Path(__file__).resolve().parent / "app.db"
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM process_event WHERE unit_id = ?", (unit_id,))
        row = cursor.fetchone()
        return int(row[0]) if row else 0


async def send_event(
    client: httpx.AsyncClient,
    station_number: int,
    unit_id: str,
    step_id: str,
) -> tuple[int, dict]:
    """Send a single event to the API endpoint."""
    payload = {
        "event_id": str(uuid4()),
        "unit_id": unit_id,
        "station_id": f"station-{station_number:02d}",
        "step_id": step_id,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
    }

    response = await client.post(ENDPOINT, json=payload)
    try:
        body = response.json()
    except ValueError:
        body = {"raw": response.text}
    return response.status_code, body


@pytest.mark.asyncio
async def test_happy_path_sequential():
    """
    Test sequential happy path: STEP-ALPHA, then STEP-BETA, then STEP-GAMMA.
    All three should return 200 "saved" in order.
    DB should have exactly 3 rows for this unit.
    """
    unit_id = f"UNIT-{uuid4().hex[:10]}"
    steps = ["STEP-ALPHA", "STEP-BETA", "STEP-GAMMA"]

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        for station_num, step_id in enumerate(steps, start=1):
            status_code, body = await send_event(client, station_num, unit_id, step_id)
            assert status_code == 200, f"Expected 200, got {status_code} for {step_id}"
            assert body.get("status") == "saved", f"Expected 'saved', got {body.get('status')}"

    db_count = count_rows_for_unit(unit_id)
    assert db_count == 3, f"Expected 3 rows in DB for unit, got {db_count}"


@pytest.mark.asyncio
async def test_concurrent_exact_duplicates():
    """
    Test concurrent idempotency: Send 10 identical replicas (same unit_id, same step_id).
    Exactly 1 should return "saved", 9 should return "duplicate_ignored".
    DB should have exactly 1 row for this step.
    """
    unit_id = f"UNIT-{uuid4().hex[:10]}"
    step_id = "STEP-ALPHA"
    station_count = 10

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        tasks = [
            send_event(client, station_number, unit_id, step_id)
            for station_number in range(1, station_count + 1)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Filter out exceptions and bad results
    responses = [r for r in results if isinstance(r, tuple)]
    assert len(responses) == station_count, f"Expected {station_count} responses, got {len(responses)}"

    saved_count = 0
    duplicate_count = 0
    for status_code, body in responses:
        assert status_code == 200, f"Expected 200, got {status_code}"
        status = body.get("status")
        if status == "saved":
            saved_count += 1
        elif status == "duplicate_ignored":
            duplicate_count += 1

    assert saved_count == 1, f"Expected 1 'saved', got {saved_count}"
    assert duplicate_count == 9, f"Expected 9 'duplicate_ignored', got {duplicate_count}"

    db_count = count_rows_for_step(unit_id, step_id)
    assert db_count == 1, f"Expected 1 row in DB, got {db_count}"


@pytest.mark.asyncio
async def test_out_of_order_delivery():
    """
    Test out-of-order delivery: Send STEP-GAMMA, wait, STEP-ALPHA, then STEP-BETA.
    All should return 200 "saved" (no 409 rejections).
    DB should have exactly 3 rows.
    """
    unit_id = f"UNIT-{uuid4().hex[:10]}"
    delivery_order = [
        ("STEP-GAMMA", 3),
        ("STEP-ALPHA", 1),
        ("STEP-BETA", 2),
    ]

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        for step_id, station_num in delivery_order:
            status_code, body = await send_event(client, station_num, unit_id, step_id)
            assert status_code == 200, f"Expected 200 for {step_id}, got {status_code}"
            assert body.get("status") == "saved", f"Expected 'saved' for {step_id}, got {body.get('status')}"
            await asyncio.sleep(0.05)

    db_count = count_rows_for_unit(unit_id)
    assert db_count == 3, f"Expected 3 rows in DB, got {db_count}"


@pytest.mark.asyncio
async def test_delayed_buffer_replay():
    """
    Test delayed buffer replay: Send STEP-ALPHA, STEP-BETA, then resend STEP-ALPHA.
    First two should return "saved", third (duplicate) should return "duplicate_ignored".
    DB should have exactly 2 rows total (not 3).
    """
    unit_id = f"UNIT-{uuid4().hex[:10]}"

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        # Send STEP-ALPHA
        status_code_1, body_1 = await send_event(client, 1, unit_id, "STEP-ALPHA")
        assert status_code_1 == 200
        assert body_1.get("status") == "saved"

        # Send STEP-BETA
        status_code_2, body_2 = await send_event(client, 2, unit_id, "STEP-BETA")
        assert status_code_2 == 200
        assert body_2.get("status") == "saved"

        # Replay STEP-ALPHA (same unit_id, same step_id, different event_id)
        status_code_3, body_3 = await send_event(client, 1, unit_id, "STEP-ALPHA")
        assert status_code_3 == 200
        assert body_3.get("status") == "duplicate_ignored"

    db_count = count_rows_for_unit(unit_id)
    assert db_count == 2, f"Expected 2 rows in DB, got {db_count}"


@pytest.mark.asyncio
async def test_concurrent_different_steps():
    """
    Test concurrent different steps on same unit:
    Send STEP-BETA and STEP-GAMMA simultaneously (no STEP-ALPHA first).
    Both should return 200 "saved".
    DB should have exactly 2 rows for this unit.
    """
    unit_id = f"UNIT-{uuid4().hex[:10]}"

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        tasks = [
            send_event(client, 1, unit_id, "STEP-BETA"),
            send_event(client, 2, unit_id, "STEP-GAMMA"),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    responses = [r for r in results if isinstance(r, tuple)]
    assert len(responses) == 2

    for status_code, body in responses:
        assert status_code == 200, f"Expected 200, got {status_code}"
        assert body.get("status") == "saved", f"Expected 'saved', got {body.get('status')}"

    db_count = count_rows_for_unit(unit_id)
    assert db_count == 2, f"Expected 2 rows in DB, got {db_count}"
