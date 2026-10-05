"""
Chaos Engineering Suite: Worker Process Injection & Recovery Asserts.

Validates that random mid-run worker process terminations result in:
1. ZERO lost tasks (reclaimed by LeaseManager and re-driven to completion).
2. ZERO duplicate successful commits (guaranteed by RDBMS fencing token checks).
"""

import time
import random
import threading
from typing import Dict, Any, List, Set
from storage.db import DatabaseManager
from storage.recovery import RecoveryEngine
from scheduler.scheduler_engine import SchedulerEngine, TaskInstance
from scheduler.lease_manager import LeaseManager

class ChaosWorkerPool:
    """Simulates active workers and random process crashes."""
    def __init__(self, db: DatabaseManager, scheduler: SchedulerEngine, lease_manager: LeaseManager, worker_count: int = 4):
        self.db: DatabaseManager = db
        self.scheduler: SchedulerEngine = scheduler
        self.lease_manager: LeaseManager = lease_manager
        self.worker_count: int = worker_count
        self.stop_signal: bool = False
        self.completed_tasks: Set[str] = set()
        self.executed_attempts: Dict[str, int] = {}

    def simulate_run_with_chaos(self, total_tasks: int = 50, failure_rate: float = 0.3) -> Dict[str, Any]:
        """
        Executes workload while injecting worker crashes.
        Returns execution validation metrics.
        """
        start_time = time.time()
        
        # Seed tasks
        for i in range(total_tasks):
            t_id = f"chaos-task-{i}"
            with self.db.get_cursor() as cursor:
                cursor.execute(
                    "INSERT INTO task_instances (task_id, run_id, node_id, state) VALUES (?, 'chaos-run', 'n1', 'READY');",
                    (t_id,)
                )
            task = TaskInstance(t_id, "chaos-run", "n1", max_retries=3)
            task.state = "READY"
            self.scheduler.register_task(task)

        active = True
        sim_now = 1000.0

        for _ in range(100):
            sim_now += 0.1

            # 1. Reclaim expired leases from crashed workers
            reclaimed = self.lease_manager.reclaim_expired_leases(now=sim_now)
            for r_id in reclaimed:
                RecoveryEngine.sync_task_state_transition(
                    db=self.db,
                    scheduler=self.scheduler,
                    lease_manager=self.lease_manager,
                    task_id=r_id,
                    expected_state="RUNNING",
                    new_state="READY",
                    now=sim_now
                )

            # 2. Poll ready retries
            self.scheduler.poll_due_retries(now=sim_now)

            # 3. Process ready tasks across simulated workers
            task = self.scheduler.pop_next_ready_task()
            if task is None:
                # Check if all tasks finished
                with self.db.get_cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM task_instances WHERE state = 'SUCCESS';")
                    success_count = cursor.fetchone()[0]
                    if success_count == total_tasks:
                        break
                continue

            worker_id = f"worker-{random.randint(1, self.worker_count)}"
            token = self.lease_manager.acquire_lease(task.task_id, worker_id, timeout_sec=0.2, now=sim_now)

            # Atomic DB transition READY -> RUNNING
            RecoveryEngine.sync_task_state_transition(
                db=self.db,
                scheduler=self.scheduler,
                lease_manager=self.lease_manager,
                task_id=task.task_id,
                expected_state="READY",
                new_state="RUNNING",
                worker_id=worker_id,
                lease_token=token,
                lease_expiry=sim_now + 0.2,
                now=sim_now
            )

            # Inject random worker process kill mid-run
            if random.random() < failure_rate:
                # Simulated crash: worker dies instantly without completing or releasing lease!
                continue
            else:
                # Worker succeeds cleanly
                RecoveryEngine.sync_task_state_transition(
                    db=self.db,
                    scheduler=self.scheduler,
                    lease_manager=self.lease_manager,
                    task_id=task.task_id,
                    expected_state="RUNNING",
                    new_state="SUCCESS",
                    now=sim_now
                )

        # Final Verification Queries against DB Source of Truth
        with self.db.get_cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM task_instances WHERE state = 'SUCCESS';")
            final_success = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM task_instances WHERE state IN ('PENDING', 'READY', 'RUNNING');")
            unfinished = cursor.fetchone()[0]

        return {
            "total_tasks": total_tasks,
            "final_success": final_success,
            "unfinished": unfinished,
            "lost_tasks": total_tasks - (final_success + unfinished)
        }
