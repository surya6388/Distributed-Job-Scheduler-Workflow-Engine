"""
Core Scheduler Engine combining Min-Heap Queues, DAGEngine, RateLimiters, and Retries.

Structures Managed:
- ready_queue: Ready Priority Queue Min-Heap (priority = task priority)
- delayed_queue: Delayed / Retry Min-Heap (priority = next_run_time epoch timestamp)
- cron_queue: Cron Triggers Min-Heap (priority = next_trigger epoch timestamp)
- dlq_store: LRU Cache Idempotency & DLQ memory store

Retries Policy:
Exponential Backoff with Jitter:
next_delay = base_delay * (2 ^ attempt) + uniform_jitter(0, 1.0)
"""

import time
import random
from typing import Dict, List, Optional, Tuple, Any
from dsa.min_heap import MinHeap, HeapNode
from dsa.lru_cache import LRUCache
from dsa.rate_limiter import TokenBucketRateLimiter
from dsa.dag import DAGEngine, DAGNode

class TaskInstance:
    __slots__ = (
        'task_id', 'run_id', 'node_id', 'state', 'priority',
        'attempt', 'max_retries', 'retry_delay_sec', 'timeout_sec'
    )

    def __init__(
        self,
        task_id: str,
        run_id: str,
        node_id: str,
        priority: int = 0,
        attempt: int = 0,
        max_retries: int = 3,
        retry_delay_sec: int = 5,
        timeout_sec: int = 30
    ):
        self.task_id: str = task_id
        self.run_id: str = run_id
        self.node_id: str = node_id
        self.state: str = 'PENDING'
        self.priority: int = priority
        self.attempt: int = attempt
        self.max_retries: int = max_retries
        self.retry_delay_sec: int = retry_delay_sec
        self.timeout_sec: int = timeout_sec

    def __repr__(self) -> str:
        return f"TaskInstance(id={self.task_id!r}, state={self.state!r}, attempt={self.attempt})"


class SchedulerEngine:
    """
    Main in-memory scheduling hub.
    """
    def __init__(self, dlq_capacity: int = 1000):
        self.ready_queue: MinHeap[TaskInstance] = MinHeap()
        self.delayed_queue: MinHeap[TaskInstance] = MinHeap()
        self.dlq_store: LRUCache[str, Dict[str, Any]] = LRUCache(capacity=dlq_capacity)
        self.dags: Dict[str, DAGEngine] = {}  # run_id -> DAGEngine
        self.tasks: Dict[str, TaskInstance] = {} # task_id -> TaskInstance
        self.rate_limiters: Dict[str, TokenBucketRateLimiter] = {} # workflow_id -> RateLimiter

    def register_dag(self, run_id: str, dag: DAGEngine) -> None:
        self.dags[run_id] = dag

    def register_task(self, task: TaskInstance) -> None:
        self.tasks[task.task_id] = task
        if task.state == 'READY':
            # Note: MinHeap pops smallest priority first, so lower numeric priority = higher dispatch priority
            self.ready_queue.push(task.task_id, float(task.priority), task)

    def compute_exponential_backoff(self, attempt: int, base_delay_sec: float) -> float:
        """Compute backoff with full jitter. O(1)."""
        backoff = base_delay_sec * (2 ** attempt)
        jitter = random.uniform(0, 1.0)
        return backoff + jitter

    def handle_task_failure(self, task_id: str, reason: str, now: Optional[float] = None) -> str:
        """
        Handle task failure. If attempt < max_retries, calculate exponential backoff
        and move to Delayed Min-Heap (RETRYING state). Otherwise, push to DLQ (DEAD state).
        """
        if task_id not in self.tasks:
            raise KeyError(f"Task '{task_id}' not found in scheduler.")

        task = self.tasks[task_id]
        task.attempt += 1
        current_time = now if now is not None else time.time()

        if task.attempt <= task.max_retries:
            task.state = 'RETRYING'
            delay = self.compute_exponential_backoff(task.attempt, float(task.retry_delay_sec))
            next_run_time = current_time + delay
            self.delayed_queue.push(task_id, next_run_time, task)
            return 'RETRYING'
        else:
            task.state = 'DEAD'
            dlq_record = {
                "task_id": task_id,
                "run_id": task.run_id,
                "reason": reason,
                "failed_at": current_time,
                "attempts": task.attempt
            }
            self.dlq_store.put(task_id, dlq_record)
            return 'DEAD'

    def poll_due_retries(self, now: Optional[float] = None) -> List[TaskInstance]:
        """
        Scan Delayed Min-Heap and move ready retries into Ready Queue. O(K log N).
        """
        current_time = now if now is not None else time.time()
        due_tasks: List[TaskInstance] = []

        while not self.delayed_queue.is_empty():
            top = self.delayed_queue.peek()
            if top is None or top.priority > current_time:
                break

            expired_node = self.delayed_queue.pop()
            task = expired_node.value
            task.state = 'READY'
            self.ready_queue.push(task.task_id, float(task.priority), task)
            due_tasks.append(task)

        return due_tasks

    def pop_next_ready_task(self, workflow_id: Optional[str] = None) -> Optional[TaskInstance]:
        """
        Pop next task from Ready Priority Queue subject to workflow rate limits. O(log N).
        """
        if self.ready_queue.is_empty():
            return None

        if workflow_id and workflow_id in self.rate_limiters:
            if not self.rate_limiters[workflow_id].allow_request():
                return None  # Rate limit exceeded for this workflow!

        node = self.ready_queue.pop()
        if node is None:
            return None
        task = node.value
        task.state = 'RUNNING'
        return task
