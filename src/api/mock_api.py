# Run this script in terminal: python3 src/api/mock_api.py
from fastapi import FastAPI # framework that allows me to build the API (brain of API)

# Endpoints for my API
'''
/health
/events
/events/{event_id}
/events/paginated
/events/fault
'''
security_events = [
  {
    "event_id": "evt_1001",
    "timestamp": "2025-10-29T13:05:44Z",
    "source_ip": "45.19.34.10",
    "destination_ip": "10.0.0.5",
    "event_type": "unauthorized_login",
    "severity": "high",
    "description": "Multiple failed SSH login attempts detected from external IP."
  },
  {
    "event_id": "evt_1002",
    "timestamp": "2025-10-29T13:11:02Z",
    "source_ip": "198.51.100.23",
    "destination_ip": "10.0.0.8",
    "event_type": "malware_detected",
    "severity": "critical",
    "description": "Antivirus flagged suspicious executable on workstation-12."
  },
  {
    "event_id": "evt_1003",
    "timestamp": "2025-10-29T13:14:55Z",
    "source_ip": "172.16.2.41",
    "destination_ip": "10.0.0.22",
    "event_type": "port_scan",
    "severity": "medium",
    "description": "Abnormal TCP SYN scan detected targeting internal subnet."
  },
  {
    "event_id": "evt_1004",
    "timestamp": "2025-10-29T13:18:19Z",
    "source_ip": "203.0.113.58",
    "destination_ip": "10.0.0.45",
    "event_type": "brute_force",
    "severity": "high",
    "description": "User account lockout triggered after repeated failed login attempts."
  },
  {
    "event_id": "evt_1005",
    "timestamp": "2025-10-29T13:22:48Z",
    "source_ip": "192.168.56.101",
    "destination_ip": "10.0.0.77",
    "event_type": "policy_violation",
    "severity": "low",
    "description": "User downloaded restricted file type from external site."
  },
  {
    "event_id": "evt_1006",
    "timestamp": "2025-10-29T13:30:01Z",
    "source_ip": "8.8.8.8",
    "destination_ip": "10.0.0.12",
    "event_type": "dns_tunnel_detected",
    "severity": "medium",
    "description": "High volume of unusual DNS queries indicative of tunneling activity."
  },
  {
    "event_id": "evt_1007",
    "timestamp": "2025-10-29T13:35:43Z",
    "source_ip": "203.0.113.220",
    "destination_ip": "10.0.0.13",
    "event_type": "data_exfiltration",
    "severity": "critical",
    "description": "Outbound data transfer exceeded expected threshold."
  },
  {
    "event_id": "evt_1008",
    "timestamp": "2025-10-29T13:40:22Z",
    "source_ip": "10.1.0.5",
    "destination_ip": "10.0.0.50",
    "event_type": "unauthorized_access",
    "severity": "high",
    "description": "Privileged access attempt detected from non-admin workstation."
  },
  {
    "event_id": "evt_1009",
    "timestamp": "2025-10-29T13:45:12Z",
    "source_ip": "198.18.4.76",
    "destination_ip": "10.0.0.100",
    "event_type": "phishing_click",
    "severity": "medium",
    "description": "Employee clicked on simulated phishing email link."
  },
  {
    "event_id": "evt_1010",
    "timestamp": "2025-10-29T13:52:09Z",
    "source_ip": "198.51.100.99",
    "destination_ip": "10.0.0.88",
    "event_type": "firewall_block",
    "severity": "low",
    "description": "Inbound connection blocked by firewall rule set."
  }
]
# FastAPI instance
app = FastAPI()


# GET request to "/"
@app.get("/")
def hello_world():
    return {"HELLO":"WORLD!"}



# GET requests to "/health"
@app.get("/health")
def api_status():
    return {"STATUS":"HEALTHY"}



@app.get("/events")
def get_events():
    return security_events


# GET request to "/event/{event_id}"
@app.get("/events/{event_id}")
def get_event_id(event_id):
    for record in security_events:
        if record["event_id"] == event_id:
            return record
        
    return {"Error":"event_id does NOT Exist"}
    

# GET request to "/events/paginated"
@app.get("/events/paginated")
def get_paginated_events(page: int, limit: int):
    start = (page - 1) * limit
    end = start + limit
    return security_events[start:end]

@app.get("/events/fault")
def get_events_fault():
    return {"status": "fault simulation placeholder"}




    







