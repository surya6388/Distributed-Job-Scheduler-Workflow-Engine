import sqlite3
import pytest
from storage.db import DatabaseManager

def test_database_schema_initialization():
    db = DatabaseManager(":memory:")
    with db.get_cursor() as cursor:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = {row[0] for row in cursor.fetchall()}

    expected_tables = {
        "workflows", "dag_nodes", "dag_edges", "workflow_runs",
        "task_instances", "task_attempts", "dead_letter_queue"
    }
    assert expected_tables.issubset(tables)

def test_analytical_queries():
    db = DatabaseManager(":memory:")
    conn = db.get_connection()

    # Populate dummy analytical data
    conn.execute("INSERT INTO workflows (workflow_id, name) VALUES ('wf1', 'Data Pipeline');")
    conn.execute("INSERT INTO dag_nodes (node_id, workflow_id, task_name) VALUES ('n1', 'wf1', 'Extract');")
    conn.execute("INSERT INTO workflow_runs (run_id, workflow_id, status) VALUES ('run1', 'wf1', 'COMPLETED');")
    conn.execute("INSERT INTO task_instances (task_id, run_id, node_id, state) VALUES ('t1', 'run1', 'n1', 'SUCCESS');")

    conn.execute("""
        INSERT INTO task_attempts (attempt_id, task_id, attempt_number, worker_id, status, duration_ms, started_at)
        VALUES 
        ('att1', 't1', 1, 'worker-1', 'SUCCESS', 150, '2026-10-06 01:00:00'),
        ('att2', 't1', 2, 'worker-2', 'FAILED', 300, '2026-10-06 01:30:00'),
        ('att3', 't1', 3, 'worker-1', 'SUCCESS', 500, '2026-10-06 02:00:00');
    """)
    conn.commit()

    analytics = db.execute_analytical_queries()
    assert "p95_duration_by_workflow" in analytics
    assert "hourly_failure_rate" in analytics
    assert len(analytics["p95_duration_by_workflow"]) > 0
    assert len(analytics["hourly_failure_rate"]) > 0
