import pytest
from storage.db import DatabaseManager
from scheduler.scheduler_engine import SchedulerEngine
from scheduler.lease_manager import LeaseManager
from chaos.chaos_monkey import ChaosWorkerPool

def test_chaos_worker_crash_no_task_loss():
    db = DatabaseManager(":memory:")
    scheduler = SchedulerEngine()
    lease_manager = LeaseManager()

    # Seed required workflow & run table records for foreign key constraints
    with db.get_cursor() as cursor:
        cursor.execute("INSERT INTO workflows (workflow_id, name) VALUES ('chaos-wf', 'Chaos Testing Workflow');")
        cursor.execute("INSERT INTO dag_nodes (node_id, workflow_id, task_name) VALUES ('n1', 'chaos-wf', 'Chaos Node');")
        cursor.execute("INSERT INTO workflow_runs (run_id, workflow_id, status) VALUES ('chaos-run', 'chaos-wf', 'RUNNING');")

    pool = ChaosWorkerPool(db, scheduler, lease_manager, worker_count=3)
    results = pool.simulate_run_with_chaos(total_tasks=30, failure_rate=0.4)

    # Assert ZERO lost tasks
    assert results["lost_tasks"] == 0
    assert results["final_success"] == 30
