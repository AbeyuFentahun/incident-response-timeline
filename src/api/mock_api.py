# Run with: python3 src/api/mock_api.py
from fastapi import FastAPI, HTTPException
from src.api.mock_event_generator import generate_valid_event

app = FastAPI()

# Stores security event
event_store = []

@app.get("/")
def home():
    return {"HELLO": "WORLD!"}


@app.get("/health")
def health():
    return {"STATUS": "HEALTHY"}


@app.get("/events")
def get_events():
    event = generate_valid_event()
    event_store.append(event)
    return event


@app.get("/events/{event_id}")
def get_event_by_id(event_id: str):
    for event in event_store:
        if event["event_id"] == event_id:
            return event

    raise HTTPException(status_code=404, detail="event_id does not exist")


@app.get("/events/paginated")
def get_paginated_events(page: int = 1, limit: int = 5):
    if page < 1:
        raise HTTPException(status_code=400, detail="Page must be >= 1")
    if limit < 1:
        raise HTTPException(status_code=400, detail="Limit must be >= 1")

    start = (page - 1) * limit
    end = start + limit

    return {
        "page": page,
        "limit": limit,
        "total": len(event_store),
        "events": event_store[start:end]
    }


@app.get("/events/batch")
def get_event_batch(size: int = 10):
    if size < 1:
        raise HTTPException(status_code=400, detail="size must be >= 1")

    batch = []

    for _ in range(size):
        event = generate_valid_event()
        event_store.append(event)
        batch.append(event)

    return {
        "size": size,
        "generated": len(batch),
        "events": batch
    }



@app.get("/events/fault")
def get_events_fault():
    return {"status": "fault simulation placeholder"}





    







