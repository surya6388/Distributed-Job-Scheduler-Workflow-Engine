"""
Worker Assignment Strategies: Round-Robin vs Least-Loaded Min-Heap.

Time Complexity:
- RoundRobinAssignment: O(1) select
- LeastLoadedAssignment: O(log W) select, O(log W) release (W = workers count)

Space Complexity:
- O(W) where W is number of workers.
"""

from typing import List, Optional
from dsa.min_heap import MinHeap

class RoundRobinAssignment:
    """Round-Robin Worker Selection Algorithm."""
    def __init__(self, worker_ids: List[str]):
        if not worker_ids:
            raise ValueError("Worker list cannot be empty.")
        self.worker_ids: List[str] = worker_ids
        self._index: int = 0

    def select_worker(self) -> str:
        worker_id = self.worker_ids[self._index]
        self._index = (self._index + 1) % len(self.worker_ids)
        return worker_id


class LeastLoadedAssignment:
    """
    Least-Loaded Worker Assignment Algorithm using a Min-Heap of worker load counts.
    Selects the worker currently executing the fewest tasks.
    """
    def __init__(self, worker_ids: List[str]):
        if not worker_ids:
            raise ValueError("Worker list cannot be empty.")
        self.heap: MinHeap[str] = MinHeap()
        self.load_map: dict[str, int] = {}

        for w_id in worker_ids:
            self.load_map[w_id] = 0
            self.heap.push(w_id, priority=0.0, value=w_id)

    def select_worker(self) -> str:
        """Assign task to least loaded worker and update heap priority. O(log W)."""
        top = self.heap.peek()
        if top is None:
            raise RuntimeError("No workers available.")
        
        worker_id = top.item_id
        self.load_map[worker_id] += 1
        self.heap.update_priority(worker_id, float(self.load_map[worker_id]))
        return worker_id

    def release_worker(self, worker_id: str) -> None:
        """Decrement active load count when task finishes. O(log W)."""
        if worker_id in self.load_map and self.load_map[worker_id] > 0:
            self.load_map[worker_id] -= 1
            self.heap.update_priority(worker_id, float(self.load_map[worker_id]))
