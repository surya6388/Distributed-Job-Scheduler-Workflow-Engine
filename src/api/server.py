"""
FastAPI REST Gateway & WebSocket Server for Real-Time Task State Telemetry.

REST Routes:
- POST /api/workflows: Create workflow and DAG topology
- POST /api/workflows/{id}/trigger: Trigger execution run
- GET  /api/workflows/{id}/runs: Query run status
- POST /api/tasks/{id}/lease: Claim ready task (Worker Protocol)
- POST /api/tasks/{id}/heartbeat: Worker heartbeat renewal
- POST /api/tasks/{id}/complete: Report task execution result
- GET  /api/analytics: Analytical SLA metrics (p95, failure rates)

WebSocket:
- WS /ws/live: Streams real-time task status transitions to React dashboard
"""

import uuid
import time
import asyncio
from typing import Dict, List, Any, Optional
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from storage.db import DatabaseManager
from storage.recovery import RecoveryEngine
from scheduler.scheduler_engine import SchedulerEngine, TaskInstance
from scheduler.lease_manager import LeaseManager
from dsa.dag import DAGEngine, DAGNode

app = FastAPI(title="Distributed Job Scheduler API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global In-Memory Shared State Hub
db_manager = DatabaseManager(":memory:")
scheduler = SchedulerEngine()
lease_manager = LeaseManager()

class ConnectionManager:
    """Manages active WebSocket connections for live UI broadcast."""
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: Dict[str, Any]):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                self.disconnect(connection)

ws_manager = ConnectionManager()

# --- Request / Response Models ---
class WorkflowCreateReq(BaseModel):
    workflow_id: str
    name: str
    cron_expression: Optional[str] = None
    nodes: list[dict[str, Any]]
    edges: list[list[str]]

class TaskCompleteReq(BaseModel):
    worker_id: str
    lease_token: str
    status: str  # 'SUCCESS' or 'FAILED'
    log_output: str = ""
    duration_ms: int = 0

# --- REST Endpoints ---

@app.post("/api/workflows")
def create_workflow(req: WorkflowCreateReq):
    dag = DAGEngine()
    for n in req.nodes:
        dag.add_node(DAGNode(
            node_id=n["node_id"],
            task_name=n["task_name"],
            cost_weight=n.get("cost_weight", 1),
            max_retries=n.get("max_retries", 3),
            retry_delay_sec=n.get("retry_delay_sec", 5)
        ))
    for p, c in req.edges:
        dag.add_edge(p, c)

    if dag.detect_cycle_dfs():
        raise HTTPException(status_code=400, detail="Workflow DAG contains a cycle.")

    with db_manager.transaction() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO workflows (workflow_id, name, cron_expression) VALUES (?, ?, ?);",
            (req.workflow_id, req.name, req.cron_expression)
        )
        for n in req.nodes:
            cursor.execute(
                "INSERT INTO dag_nodes (node_id, workflow_id, task_name, cost_weight) VALUES (?, ?, ?, ?);",
                (n["node_id"], req.workflow_id, n["task_name"], n.get("cost_weight", 1))
            )
        for p, c in req.edges:
            cursor.execute(
                "INSERT INTO dag_edges (workflow_id, parent_node_id, child_node_id) VALUES (?, ?, ?);",
                (req.workflow_id, p, c)
            )

    return {"status": "created", "workflow_id": req.workflow_id}


@app.post("/api/workflows/{workflow_id}/trigger")
async def trigger_workflow(workflow_id: str):
    run_id = f"run-{uuid.uuid4().hex[:8]}"

    with db_manager.get_cursor() as cursor:
        cursor.execute("SELECT node_id, task_name, cost_weight, max_retries FROM dag_nodes WHERE workflow_id = ?;", (workflow_id,))
        nodes = cursor.fetchall()
        if not nodes:
            raise HTTPException(status_code=404, detail="Workflow not found.")

        cursor.execute("SELECT parent_node_id, child_node_id FROM dag_edges WHERE workflow_id = ?;", (workflow_id,))
        edges = cursor.fetchall()

        cursor.execute("INSERT INTO workflow_runs (run_id, workflow_id, status) VALUES (?, ?, 'RUNNING');", (run_id, workflow_id))

        dag = DAGEngine()
        for n in nodes:
            dag.add_node(DAGNode(n["node_id"], n["task_name"], cost_weight=n["cost_weight"]))
        for p, c in edges:
            dag.add_edge(p, c)

        scheduler.register_dag(run_id, dag)

        # Create Task Instances in READY or PENDING depending on in-degree count
        ready_tasks = []
        for n in nodes:
            node_id = n["node_id"]
            in_deg = dag.in_degree[node_id]
            state = "READY" if in_deg == 0 else "PENDING"
            task_id = f"task-{run_id}-{node_id}"

            cursor.execute(
                "INSERT INTO task_instances (task_id, run_id, node_id, state) VALUES (?, ?, ?, ?);",
                (task_id, run_id, node_id, state)
            )
            task = TaskInstance(task_id, run_id, node_id, max_retries=n["max_retries"])
            task.state = state
            scheduler.register_task(task)

            if state == "READY":
                ready_tasks.append(task_id)

    await ws_manager.broadcast({"event": "RUN_STARTED", "run_id": run_id, "workflow_id": workflow_id})
    return {"run_id": run_id, "status": "RUNNING", "initial_ready": ready_tasks}


@app.post("/api/tasks/claim")
def claim_task(worker_id: str, timeout_sec: float = 30.0):
    task = scheduler.pop_next_ready_task()
    if not task:
        return {"claimed": False, "task": None}

    now = time.time()
    lease_token = lease_manager.acquire_lease(task.task_id, worker_id, timeout_sec, now=now)
    
    RecoveryEngine.sync_task_state_transition(
        db=db_manager,
        scheduler=scheduler,
        lease_manager=lease_manager,
        task_id=task.task_id,
        expected_state="READY",
        new_state="RUNNING",
        worker_id=worker_id,
        lease_token=lease_token,
        lease_expiry=now + timeout_sec,
        now=now
    )

    return {
        "claimed": True,
        "task_id": task.task_id,
        "run_id": task.run_id,
        "node_id": task.node_id,
        "lease_token": lease_token,
        "timeout_sec": timeout_sec
    }


@app.post("/api/tasks/{task_id}/complete")
async def complete_task(task_id: str, req: TaskCompleteReq):
    now = time.time()
    if task_id not in scheduler.tasks:
        raise HTTPException(status_code=404, detail="Task not found.")

    task = scheduler.tasks[task_id]

    if req.status == "SUCCESS":
        RecoveryEngine.sync_task_state_transition(
            db=db_manager,
            scheduler=scheduler,
            lease_manager=lease_manager,
            task_id=task_id,
            expected_state="RUNNING",
            new_state="SUCCESS",
            now=now
        )
        # Unlock downstream DAG dependencies
        dag = scheduler.dags.get(task.run_id)
        if dag:
            for child_id in dag.adj_list.get(task.node_id, []):
                # Decrement in-degree
                dag.in_degree[child_id] -= 1
                if dag.in_degree[child_id] == 0:
                    child_task_id = f"task-{task.run_id}-{child_id}"
                    RecoveryEngine.sync_task_state_transition(
                        db=db_manager,
                        scheduler=scheduler,
                        lease_manager=lease_manager,
                        task_id=child_task_id,
                        expected_state="PENDING",
                        new_state="READY",
                        now=now
                    )

        await ws_manager.broadcast({"event": "TASK_SUCCESS", "task_id": task_id, "run_id": task.run_id})
        return {"status": "SUCCESS"}
    else:
        new_state = scheduler.handle_task_failure(task_id, req.log_output, now=now)
        RecoveryEngine.sync_task_state_transition(
            db=db_manager,
            scheduler=scheduler,
            lease_manager=lease_manager,
            task_id=task_id,
            expected_state="RUNNING",
            new_state=new_state,
            now=now
        )
        await ws_manager.broadcast({"event": f"TASK_{new_state}", "task_id": task_id, "run_id": task.run_id})
        return {"status": new_state}


@app.get("/api/analytics")
def get_analytics():
    return db_manager.execute_analytical_queries()

@app.websocket("/ws/live")
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
