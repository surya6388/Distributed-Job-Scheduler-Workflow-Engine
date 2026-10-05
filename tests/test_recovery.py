import time
import pytest
from storage.db import DatabaseManager
from storage.recovery import RecoveryEngine
from scheduler.scheduler_engine import SchedulerEngine, TaskInstance
from scheduler.lease_manager import LeaseManager

def test_commit_then_apply_state_transition():
    db = DatabaseManager(":memory:")
    scheduler = SchedulerEngine()
    lease_manager = LeaseManager()

    # Seed DB with task instance
    with db.get_cursor() as cursor:
        cursor.execute("INSERT INTO workflows (workflow_id, name) VALUES ('wf1', 'Test WF');")
        cursor.execute("INSERT INTO dag_nodes (node_id, workflow_id, task_name) VALUES ('n1', 'wf1', 'Node 1');")
        cursor.execute("INSERT INTO workflow_runs (run_id, workflow_id, status) VALUES ('run1', 'wf1', 'RUNNING');")
        cursor.execute("INSERT INTO task_instances (task_id, run_id, node_id, state) VALUES ('t1', 'run1', 'n1', 'READY');")

    task = TaskInstance("t1", "run1", "n1")
    task.state = "READY"
    scheduler.register_task(task)

    now = 1000.0
    # Perform atomic state transition READY -> RUNNING
    success = RecoveryEngine.sync_task_state_transition(
        db=db,
        scheduler=scheduler,
        lease_manager=lease_manager,
        task_id="t1",
        expected_state="READY",
        new_state="RUNNING",
        worker_id="worker-99",
        lease_token="token-abc",
        lease_expiry=now + 30.0,
        now=now
    )

    assert success is True
    assert task.state == "RUNNING"
    assert "t1" in lease_manager.lease_map

    # Verify RDBMS updated state
    with db.get_cursor() as cursor:
        cursor.execute("SELECT state, worker_id FROM task_instances WHERE task_id = 't1';")
        row = dict(cursor.fetchone())
        assert row["state"] == "RUNNING"
        assert row["worker_id"] == "worker-99"

def test_cold_start_recovery_rebuild():
    db = DatabaseManager(":memory:")
    now = 1000.0

    # Seed database with unfinished tasks from prior run
    with db.get_cursor() as cursor:
        cursor.execute("INSERT INTO workflows (workflow_id, name) VALUES ('wf1', 'Recovery WF');")
        cursor.execute("INSERT INTO dag_nodes (node_id, workflow_id, task_name) VALUES ('n1', 'wf1', 'T1');")
        cursor.execute("INSERT INTO workflow_runs (run_id, workflow_id, status) VALUES ('run1', 'wf1', 'RUNNING');")
        
        # Ready task
        cursor.execute("INSERT INTO task_instances (task_id, run_id, node_id, state, priority) VALUES ('t_ready', 'run1', 'n1', 'READY', 1);")
        # Valid running task (expires t=1050 > now=1000)
        cursor.execute("INSERT INTO task_instances (task_id, run_id, node_id, state, worker_id, lease_expiry) VALUES ('t_running_valid', 'run1', 'n1', 'RUNNING', 'w1', 1050.0);")
        # Expired running task (expired t=990 < now=1000) -> should be reclaimed to READY
        cursor.execute("INSERT INTO task_instances (task_id, run_id, node_id, state, worker_id, lease_expiry) VALUES ('t_running_expired', 'run1', 'n1', 'RUNNING', 'w2', 990.0);")

    new_scheduler = SchedulerEngine()
    new_lease_manager = LeaseManager()

    counts = RecoveryEngine.cold_start_rebuild(db, new_scheduler, new_lease_manager, now=now)

    assert counts["READY"] == 1
    assert counts["RUNNING"] == 1
    assert counts["RECLAIMED"] == 1

    # Verify reclaimed task is now in Ready Queue
    assert new_scheduler.tasks["t_running_expired"].state == "READY"
    assert "t_running_valid" in new_lease_manager.lease_map
