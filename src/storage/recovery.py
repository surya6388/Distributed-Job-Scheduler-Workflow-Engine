"""
Crash Recovery & Commit-Then-Apply Protocol Engine.

Strict Ordering Rules:
1. Transaction Commit First: Execute SQL update against durable RDBMS WAL.
2. In-Memory State Apply Second: Update in-memory Min-Heaps, Lease Map, and DAG state ONLY after DB commit succeeds.
3. Cold-Start Rebuild: Reconstructs in-memory data structures from DB state on Master process restart.

Time Complexity:
- sync_task_state_transition: O(log N) after O(1) DB commit
- cold_start_rebuild: O(N log N) scan and populate

Space Complexity:
- O(N) where N is active tasks count.
"""

import time
from typing import Dict, Any, List, Optional
from storage.db import DatabaseManager
from scheduler.scheduler_engine import SchedulerEngine, TaskInstance
from scheduler.lease_manager import LeaseManager

class RecoveryEngine:
    """
    Manages state synchronization between DB durable store and in-memory engine.
    Handles startup recovery reconciliation.
    """
    @staticmethod
    def sync_task_state_transition(
        db: DatabaseManager,
        scheduler: SchedulerEngine,
        lease_manager: LeaseManager,
        task_id: str,
        expected_state: str,
        new_state: str,
        worker_id: Optional[str] = None,
        lease_token: Optional[str] = None,
        lease_expiry: Optional[float] = None,
        next_run_time: Optional[float] = None,
        now: Optional[float] = None
    ) -> bool:
        """
        Commit-Then-Apply State Transition.
        Step 1: Execute atomic SQL statement.
        Step 2: Mutate in-memory structures only upon DB success.
        """
        current_time = now if now is not None else time.time()

        # Step 1: Execute RDBMS Write First
        with db.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE task_instances
                SET state = ?, worker_id = ?, lease_token = ?, lease_expiry = ?, next_run_time = ?, updated_at = CURRENT_TIMESTAMP
                WHERE task_id = ? AND state = ?;
                """,
                (new_state, worker_id, lease_token, lease_expiry, next_run_time, task_id, expected_state)
            )
            if cursor.rowcount == 0:
                return False  # CAS check failed! Concurrent transition occurred.

        # Step 2: In-Memory State Mutation Second
        if task_id in scheduler.tasks:
            task = scheduler.tasks[task_id]
            task.state = new_state

            if new_state == 'RUNNING' and worker_id and lease_expiry:
                lease_manager.acquire_lease(task_id, worker_id, lease_expiry - current_time, now=current_time)
            elif new_state == 'SUCCESS' or new_state == 'DEAD':
                if task_id in lease_manager.lease_map:
                    stored_token = lease_manager.lease_map[task_id][2]
                    lease_manager.release_lease(task_id, stored_token)
            elif new_state == 'READY':
                scheduler.ready_queue.push(task_id, float(task.priority), task)

        return True

    @staticmethod
    def cold_start_rebuild(
        db: DatabaseManager,
        scheduler: SchedulerEngine,
        lease_manager: LeaseManager,
        now: Optional[float] = None
    ) -> Dict[str, int]:
        """
        Cold-Start Startup Rebuild.
        Reads all active tasks from DB and populates in-memory Min-Heaps and Lease Map.
        O(N log N) time.
        """
        current_time = now if now is not None else time.time()
        counts = {"READY": 0, "RUNNING": 0, "RETRYING": 0, "RECLAIMED": 0}

        with db.get_cursor() as cursor:
            cursor.execute(
                """
                SELECT task_id, run_id, node_id, state, priority, attempt, max_retries, 
                       worker_id, lease_token, lease_expiry, next_run_time
                FROM task_instances
                WHERE state IN ('READY', 'RUNNING', 'RETRYING');
                """
            )
            rows = cursor.fetchall()

            for row in rows:
                row_dict = dict(row)
                task_id = row_dict["task_id"]
                state = row_dict["state"]
                lease_expiry = row_dict["lease_expiry"]

                task = TaskInstance(
                    task_id=task_id,
                    run_id=row_dict["run_id"],
                    node_id=row_dict["node_id"],
                    priority=row_dict["priority"],
                    attempt=row_dict["attempt"],
                    max_retries=row_dict["max_retries"]
                )
                task.state = state
                scheduler.register_task(task)

                if state == 'READY':
                    counts["READY"] += 1

                elif state == 'RUNNING':
                    if lease_expiry and float(lease_expiry) > current_time:
                        # Active valid lease
                        remaining_ttl = float(lease_expiry) - current_time
                        lease_manager.acquire_lease(task_id, row_dict["worker_id"], remaining_ttl, now=current_time)
                        counts["RUNNING"] += 1
                    else:
                        # Lease expired during Master downtime -> Reclaim to READY
                        cursor.execute(
                            "UPDATE task_instances SET state = 'READY', worker_id = NULL, lease_token = NULL WHERE task_id = ?;",
                            (task_id,)
                        )
                        task.state = 'READY'
                        scheduler.ready_queue.push(task_id, float(task.priority), task)
                        counts["RECLAIMED"] += 1

                elif state == 'RETRYING':
                    next_run = float(row_dict["next_run_time"]) if row_dict["next_run_time"] else current_time
                    scheduler.delayed_queue.push(task_id, next_run, task)
                    counts["RETRYING"] += 1

        return counts
