import asyncio
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import httpx

BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000")
ENDPOINT = "/api/events"
STATION_COUNT = 10


def count_rows_for_step(unit_id: str, step_id: str) -> int:
    db_path = Path(__file__).resolve().parent / "app.db"
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM process_event WHERE unit_id = ? AND step_id = ?",
            (unit_id, step_id),
        )
        row = cursor.fetchone()
        return int(row[0]) if row else 0


async def send_event(
    client: httpx.AsyncClient,
    station_number: int,
    unit_id: str,
    step_id: str,
) -> tuple[int, dict]:
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


async def main() -> None:
    # Use a fresh unit_id each run to make results deterministic.
    unit_id = f"UNIT-{uuid4().hex[:10]}"
    step_id = "STEP-ALPHA"

    print(f"Target endpoint: {BASE_URL}{ENDPOINT}")
    print(f"Simulating {STATION_COUNT} stations for unit_id={unit_id}, step_id={step_id}")

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        tasks = [
            send_event(client, station_number, unit_id, step_id)
            for station_number in range(1, STATION_COUNT + 1)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    exceptions = [r for r in results if isinstance(r, Exception)]
    responses = [r for r in results if not isinstance(r, Exception)]

    if exceptions:
        print("Request exceptions encountered:")
        for exc in exceptions:
            print(f"- {exc}")

    status_codes = [status for status, _ in responses]
    non_200 = [code for code in status_codes if code != 200]

    saved = 0
    duplicate_ignored = 0
    for _, body in responses:
        if isinstance(body, dict) and body.get("status") == "saved":
            saved += 1
        elif isinstance(body, dict) and body.get("status") == "duplicate_ignored":
            duplicate_ignored += 1

    db_count = count_rows_for_step(unit_id, step_id)

    print("\n=== Simulation Summary ===")
    print(f"Total requests attempted: {STATION_COUNT}")
    print(f"Successful HTTP 200 responses: {len(status_codes) - len(non_200)}")
    print(f"Non-200 responses: {len(non_200)}")
    print(f"Backend reported 'saved': {saved}")
    print(f"Backend reported 'duplicate_ignored': {duplicate_ignored}")
    print(f"Rows in DB for (unit_id, step_id): {db_count}")

    if len(non_200) == 0 and db_count == 1:
        print("PASS: Idempotency holds under concurrent load.")
    else:
        print("FAIL: Unexpected status codes or duplicate DB rows detected.")


if __name__ == "__main__":
    asyncio.run(main())
