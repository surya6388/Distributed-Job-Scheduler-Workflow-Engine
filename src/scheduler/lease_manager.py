"""
Hand-Built Lease Manager & Dead-Worker Task Reclamation Protocol.

Uses custom MinHeap to manage lease expiration timestamps in O(log N) time
and an in-memory Hash Map for O(1) lease token validation.

Time Complexity:
- acquire_lease: O(log N)
- renew_heartbeat: O(log N)
- reclaim_expired_leases: O(K log N) where K is expired lease count
- release_lease: O(log N)

Space Complexity:
- O(N) where N is active running tasks count.
"""

import time
import secrets
from typing import Dict, Tuple, List, Optional
from dsa.min_heap import MinHeap

class LeaseManager:
    """
    Manages task worker leases and heartbeat renewals.
    Detects worker failures and reclaims orphaned tasks via Expiry Min-Heap.
    """
    def __init__(self):
        # task_id -> (worker_id, expiry_time, lease_token)
        self.lease_map: Dict[str, Tuple[str, float, str]] = {}
        # Min-Heap keyed by expiry_time
        self.expiry_heap: MinHeap[str] = MinHeap()

    def acquire_lease(self, task_id: str, worker_id: str, timeout_sec: float, now: Optional[float] = None) -> str:
        """
        Assign timed lease to worker_id and return a cryptographic fencing lease_token. O(log N).
        """
        current_time = now if now is not None else time.time()
        expiry_time = current_time + timeout_sec
        lease_token = secrets.token_hex(16)

        if task_id in self.lease_map:
            # Re-lease existing task
            self.release_lease(task_id, self.lease_map[task_id][2])

        self.lease_map[task_id] = (worker_id, expiry_time, lease_token)
        self.expiry_heap.push(task_id, expiry_time, task_id)
        return lease_token

    def renew_heartbeat(self, task_id: str, lease_token: str, extension_sec: float, now: Optional[float] = None) -> bool:
        """
        Extend lease expiry for active worker if lease_token matches. O(log N).
        Fencing token prevents late heartbeats from dead workers.
        """
        if task_id not in self.lease_map:
            return False

        worker_id, current_expiry, stored_token = self.lease_map[task_id]
        if stored_token != lease_token:
            return False  # Fencing token mismatch! Worker lost its lease.

        current_time = now if now is not None else time.time()
        if current_time >= current_expiry:
            return False  # Already expired!

        new_expiry = current_time + extension_sec
        self.lease_map[task_id] = (worker_id, new_expiry, stored_token)
        self.expiry_heap.update_priority(task_id, new_expiry)
        return True

    def release_lease(self, task_id: str, lease_token: str) -> bool:
        """Release lease upon successful task completion. O(log N)."""
        if task_id not in self.lease_map:
            return False
        _, _, stored_token = self.lease_map[task_id]
        if stored_token != lease_token:
            return False

        del self.lease_map[task_id]
        self.expiry_heap.remove(task_id)
        return True

    def reclaim_expired_leases(self, now: Optional[float] = None) -> List[str]:
        """
        Scan Expiry Min-Heap and reclaim tasks from crashed workers whose lease expired. O(K log N).
        Returns list of reclaimed task_ids.
        """
        current_time = now if now is not None else time.time()
        reclaimed: List[str] = []

        while not self.expiry_heap.is_empty():
            top = self.expiry_heap.peek()
            if top is None or top.priority > current_time:
                break  # Earliest expiry is still in the future

            expired_node = self.expiry_heap.pop()
            task_id = expired_node.item_id
            if task_id in self.lease_map:
                del self.lease_map[task_id]
                reclaimed.append(task_id)

        return reclaimed
