"""
Raw SQL Database Layer (SQLite WAL Mode & Raw SQL Execution).
No ORM abstractions used. Direct transactional execution with psycopg/sqlite3 compatibility.

Guarantees:
- WAL mode for concurrent readers and single writer
- Strict foreign key enforcement
- Thread-safe connection management
"""

import sqlite3
import os
import contextlib
from typing import Generator, List, Dict, Any, Tuple, Optional

DB_SCHEMA_SQL = """
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

-- Workflows Definition Table
CREATE TABLE IF NOT EXISTS workflows (
    workflow_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    cron_expression TEXT,
    concurrency_limit INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DAG Node Definitions
CREATE TABLE IF NOT EXISTS dag_nodes (
    node_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id) ON DELETE CASCADE,
    task_name TEXT NOT NULL,
    max_retries INTEGER DEFAULT 3,
    retry_delay_sec INTEGER DEFAULT 5,
    timeout_sec INTEGER DEFAULT 30,
    cost_weight INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DAG Edges (Dependencies)
CREATE TABLE IF NOT EXISTS dag_edges (
    workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id) ON DELETE CASCADE,
    parent_node_id TEXT NOT NULL REFERENCES dag_nodes(node_id),
    child_node_id TEXT NOT NULL REFERENCES dag_nodes(node_id),
    PRIMARY KEY (workflow_id, parent_node_id, child_node_id)
);

-- Workflow Execution Runs
CREATE TABLE IF NOT EXISTS workflow_runs (
    run_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL REFERENCES workflows(workflow_id),
    status TEXT NOT NULL CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED')),
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP
);

-- Task Execution Instances
CREATE TABLE IF NOT EXISTS task_instances (
    task_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
    node_id TEXT NOT NULL REFERENCES dag_nodes(node_id),
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'READY', 'RUNNING', 'SUCCESS', 'FAILED', 'RETRYING', 'DEAD')),
    priority INTEGER DEFAULT 0,
    attempt INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    worker_id TEXT,
    lease_token TEXT,
    lease_expiry TIMESTAMP,
    next_run_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Task Attempt Logs
CREATE TABLE IF NOT EXISTS task_attempts (
    attempt_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES task_instances(task_id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL,
    worker_id TEXT NOT NULL,
    status TEXT NOT NULL,
    log_output TEXT,
    duration_ms INTEGER DEFAULT 0,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP
);

-- Dead Letter Queue Persistent Table
CREATE TABLE IF NOT EXISTS dead_letter_queue (
    dlq_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL UNIQUE REFERENCES task_instances(task_id),
    run_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    payload TEXT,
    failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Recovery & Performance Indexes
CREATE INDEX IF NOT EXISTS idx_task_instances_recovery 
ON task_instances(state, lease_expiry) 
WHERE state IN ('READY', 'RUNNING', 'RETRYING');

CREATE INDEX IF NOT EXISTS idx_task_instances_run_state 
ON task_instances(run_id, state);

CREATE INDEX IF NOT EXISTS idx_task_attempts_task 
ON task_attempts(task_id, attempt_number);
"""

class DatabaseManager:
    """
    Manages SQLite database connections and raw SQL execution.
    """
    def __init__(self, db_path: str = "scheduler.db"):
        self.db_path: str = db_path
        self._shared_conn: Optional[sqlite3.Connection] = None
        if db_path == ":memory:":
            self._shared_conn = sqlite3.connect(":memory:", check_same_thread=False)
            self._shared_conn.row_factory = sqlite3.Row
            self._shared_conn.execute("PRAGMA foreign_keys = ON;")
        self._init_db()

    def get_connection(self) -> sqlite3.Connection:
        if self._shared_conn is not None:
            return self._shared_conn
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        return conn

    @contextlib.contextmanager
    def transaction(self) -> Generator[sqlite3.Connection, None, None]:
        conn = self.get_connection()
        try:
            if not conn.in_transaction:
                conn.execute("BEGIN IMMEDIATE;")
            yield conn
            if conn != self._shared_conn:
                conn.commit()
        except Exception:
            if conn != self._shared_conn:
                conn.rollback()
            raise
        finally:
            if conn != self._shared_conn:
                conn.close()

    @contextlib.contextmanager
    def get_cursor(self) -> Generator[sqlite3.Cursor, None, None]:
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            yield cursor
            if conn != self._shared_conn:
                conn.commit()
        except Exception:
            if conn != self._shared_conn:
                conn.rollback()
            raise
        finally:
            cursor.close()
            if conn != self._shared_conn:
                conn.close()

    def _init_db(self) -> None:
        conn = self.get_connection()
        try:
            conn.executescript(DB_SCHEMA_SQL)
            if conn != self._shared_conn:
                conn.commit()
        finally:
            if conn != self._shared_conn:
                conn.close()

    def execute_analytical_queries(self) -> Dict[str, Any]:
        """
        Runs analytics: Task duration p95 by workflow, and hourly failure rate using Window functions.
        """
        with self.get_cursor() as cursor:
            # Query 1: Task duration p95 per workflow using NTILE / Window functions
            query_p95 = """
            WITH TaskDurations AS (
                SELECT 
                    w.workflow_id,
                    w.name as workflow_name,
                    ta.duration_ms,
                    ROW_NUMBER() OVER (PARTITION BY w.workflow_id ORDER BY ta.duration_ms) as row_num,
                    COUNT(*) OVER (PARTITION BY w.workflow_id) as total_count
                FROM task_attempts ta
                JOIN task_instances ti ON ta.task_id = ti.task_id
                JOIN workflow_runs wr ON ti.run_id = wr.run_id
                JOIN workflows w ON wr.workflow_id = w.workflow_id
            )
            SELECT workflow_id, workflow_name, MAX(duration_ms) as p95_duration_ms
            FROM TaskDurations
            WHERE row_num >= CAST(0.95 * total_count AS INT)
            GROUP BY workflow_id, workflow_name;
            """
            cursor.execute(query_p95)
            p95_results = [dict(row) for row in cursor.fetchall()]

            # Query 2: Hourly failure rate using window functions
            query_failure_rate = """
            SELECT 
                strftime('%Y-%m-%d %H:00:00', started_at) as hour_bucket,
                COUNT(*) as total_attempts,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_attempts,
                ROUND(100.0 * SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) / COUNT(*), 2) as failure_rate_pct
            FROM task_attempts
            GROUP BY hour_bucket
            ORDER BY hour_bucket DESC;
            """
            cursor.execute(query_failure_rate)
            failure_results = [dict(row) for row in cursor.fetchall()]

            return {
                "p95_duration_by_workflow": p95_results,
                "hourly_failure_rate": failure_results
            }
