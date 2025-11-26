# To start the API server:
# uvicorn src.api.mock_api:app --reload --host 0.0.0.0 --port 8000
import random
import os
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Depends, Header
from src.api.mock_event_generator import generate_valid_event, generate_invalid_event

"""
Mock Security Event Ingestion API
---------------------------------
This FastAPI service simulates a real-world log ingestion endpoint used by
SIEM / EDR / SOC pipelines. It supports secure ingestion of valid and invalid
security events, batch generation, pagination, and diagnostic endpoints.

Used by the `incident_response_timeline` ETL pipeline during the Extract phase.
"""

app = FastAPI()

# In-memory event store – persists only while the API is running.
# Valid events from batch ingestion are appended here.
event_store = []


# ---------------------------------------------------------------------------
# API KEY AUTHENTICATION
# ---------------------------------------------------------------------------

# Load environment file into OS memory
load_dotenv("config/.env")

# Access environmental variables 
API_KEY = os.getenv("API_KEY")

# Fail-fast if API_KEY was not found
if not API_KEY:
    raise RuntimeError("API_KEY environment variable is missing. Check config/.env.")


# Verifies that the incoming request contains the correct API key.
# All ingestion endpoints use this dependency.
def verify_api_key(x_api_key: str = Header(None)):

    if x_api_key != API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API Key"
        )
    return True


# ---------------------------------------------------------------------------
# BASIC ENDPOINTS (No Authentication)
# ---------------------------------------------------------------------------


# Simple home route for smoke testing.
@app.get("/")
def home():
    return {"HELLO": "WORLD!"}

# Health check endpoint.
# Unprotected so Docker/Kubernetes/Airflow can probe it freely.
@app.get("/health")
def health():

    return {"STATUS": "HEALTHY"}


# ---------------------------------------------------------------------------
# EVENT GENERATION ENDPOINTS
# ---------------------------------------------------------------------------

# Generate a single valid event and store it.
# Used for unit tests and small ingestion checks.
@app.get("/events", dependencies=[Depends(verify_api_key)])
def get_events():

    event = generate_valid_event()
    event_store.append(event)
    return event



# Paginate stored events. Simulates a SIEM browsing interface.
@app.get("/events/paginated", dependencies=[Depends(verify_api_key)])
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


# ---------------------------------------------------------------------------
# BATCH INGESTION ENDPOINT (Primary Extract Entry Point)
# ---------------------------------------------------------------------------

# Generate a batch of events.
# - Valid events are generated and stored.
# - Invalid events are generated but *not* stored.
# - Returns a shuffled mix of events to simulate unordered log arrival.
# Used by extract_security_events.py during ETL Extract phase.
@app.get("/events/batch", dependencies=[Depends(verify_api_key)])
def get_events_batch(size: int = 10, fault_rate: float = 0.0):

    # Validate input
    if size < 1:
        raise HTTPException(status_code=400, detail="size must be >= 1")
    if not (0.0 <= fault_rate <= 1.0):
        raise HTTPException(status_code=400, detail="fault_rate must be between 0.0 and 1.0")

    # Determine number of valid vs invalid events
    invalid_count = int(size * fault_rate)
    valid_count = size - invalid_count

    events = []

    # Generate valid events
    for _ in range(valid_count):
        event = generate_valid_event()
        events.append(event)
        event_store.append(event)   # Only valid events are stored

    # Generate invalid events
    for _ in range(invalid_count):
        events.append(generate_invalid_event())

    # Simulate real-world unordered arrival
    random.shuffle(events)

    return {
        "size": size,
        "fault_rate": fault_rate,
        "valid_events": valid_count,
        "invalid_events": invalid_count,
        "events": events
    }


# ---------------------------------------------------------------------------
# OBSERVABILITY & MANAGEMENT ENDPOINTS
# ---------------------------------------------------------------------------

# Returns metadata about the current event store.
# Useful for monitoring, debugging, and metrics tracking.
@app.get("/events/stats", dependencies=[Depends(verify_api_key)])
def get_event_stats():

    total = len(event_store)
    unique_ids = len({e["event_id"] for e in event_store})

    return {
        "total_events": total,
        "unique_event_ids": unique_ids
    }


# Clears the in-memory event store.
# NOTE: Only for local testing / development.
# Do NOT include in production APIs.
@app.delete("/events/clear", dependencies=[Depends(verify_api_key)])
def clear_event_store():

    count = len(event_store)
    event_store.clear()

    return {
        "status": "success",
        "cleared_events": count
    }


# ---------------------------------------------------------------------------
# THIS ROUTE MUST ALWAYS BE LAST (dynamic route) 
# FASTAPI tries the routes in the order they are defined and /events/{event_id} tries to match anything after /events/.
# ---------------------------------------------------------------------------

# Retrieve a single event from the in-memory store.
# Useful for debugging and validation training.
@app.get("/events/{event_id}", dependencies=[Depends(verify_api_key)])
def get_event_by_id(event_id: str):

    for event in event_store:
        if event["event_id"] == event_id:
            return event

    raise HTTPException(
        status_code=404,
        detail="event_id does not exist"
    )





    







