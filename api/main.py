import json
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware


RECOMMENDATIONS_PATH = Path(os.getenv("RECOMMENDATIONS_PATH", "data/recommendations.json"))
STREAMING_EVENTS_PATH = Path(os.getenv("STREAMING_EVENTS_PATH", "data/streaming_events"))

app = FastAPI(title="MNP Recommendation API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def load_recommendations() -> dict:
    if not RECOMMENDATIONS_PATH.exists():
        return {}

    with RECOMMENDATIONS_PATH.open("r", encoding="utf-8") as file:
        return json.load(file)


def load_recent_events(limit: int = 20) -> list[dict]:
    if not STREAMING_EVENTS_PATH.exists():
        return []

    event_files = sorted(
        STREAMING_EVENTS_PATH.glob("part-*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    events = []
    for event_file in event_files:
        with event_file.open("r", encoding="utf-8") as file:
            for line in file:
                if not line.strip():
                    continue
                events.append(json.loads(line))
                if len(events) >= limit:
                    return events

    return events


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/recommendations/user/{user_id}")
def get_user_recommendations(user_id: str) -> dict:
    data = load_recommendations()
    recommendations = data.get(str(user_id))

    if recommendations is None:
        raise HTTPException(status_code=404, detail="No recommendations found for this user")

    return {
        "user_id": user_id,
        "recommendations": recommendations,
    }


@app.get("/events/recent")
def get_recent_events(limit: int = 20) -> dict:
    if limit < 1 or limit > 100:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 100")

    return {"events": load_recent_events(limit)}
