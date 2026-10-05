import pytest
from fastapi.testclient import TestClient
from api.server import app

client = TestClient(app)

def test_api_workflow_lifecycle():
    # 1. Create Workflow
    wf_payload = {
        "workflow_id": "api-wf-1",
        "name": "ETL Pipeline",
        "cron_expression": "0 * * * *",
        "nodes": [
            {"node_id": "extract", "task_name": "Extract DB", "cost_weight": 2},
            {"node_id": "transform", "task_name": "Transform Spark", "cost_weight": 5},
            {"node_id": "load", "task_name": "Load Warehouse", "cost_weight": 3}
        ],
        "edges": [
            ["extract", "transform"],
            ["transform", "load"]
        ]
    }
    response = client.post("/api/workflows", json=wf_payload)
    assert response.status_code == 200
    assert response.json()["status"] == "created"

    # 2. Trigger Workflow Run
    trigger_resp = client.post("/api/workflows/api-wf-1/trigger")
    assert trigger_resp.status_code == 200
    run_data = trigger_resp.json()
    assert run_data["status"] == "RUNNING"
    assert len(run_data["initial_ready"]) == 1
    assert "extract" in run_data["initial_ready"][0]

    # 3. Worker Claims Task
    claim_resp = client.post("/api/tasks/claim?worker_id=worker-01")
    assert claim_resp.status_code == 200
    claimed = claim_resp.json()
    assert claimed["claimed"] is True
    assert claimed["node_id"] == "extract"

    # 4. Worker Completes Extract Task
    complete_payload = {
        "worker_id": "worker-01",
        "lease_token": claimed["lease_token"],
        "status": "SUCCESS",
        "log_output": "Extracted 5000 rows",
        "duration_ms": 250
    }
    comp_resp = client.post(f"/api/tasks/{claimed['task_id']}/complete", json=complete_payload)
    assert comp_resp.status_code == 200
    assert comp_resp.json()["status"] == "SUCCESS"

    # 5. Claim Next Unlocked Task (Transform)
    claim2_resp = client.post("/api/tasks/claim?worker_id=worker-02")
    assert claim2_resp.status_code == 200
    claimed2 = claim2_resp.json()
    assert claimed2["claimed"] is True
    assert claimed2["node_id"] == "transform"
