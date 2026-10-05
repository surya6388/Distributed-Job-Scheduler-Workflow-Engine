"""
Custom Binary Min-Heap Implementation with O(1) Index Lookup and O(log N) Decrease-Key / Remove.

Time Complexity:
- push: O(log N)
- pop: O(log N)
- peek: O(1)
- decrease_key / update_priority: O(log N)
- remove: O(log N)
- contains: O(1)
- size: O(1)

Space Complexity:
- O(N) for array storage + index hash map.
"""

from typing import Any, Generic, TypeVar, Optional, Tuple

T = TypeVar('T')

class HeapNode(Generic[T]):
    __slots__ = ('item_id', 'priority', 'value')

    def __init__(self, item_id: str, priority: float, value: T):
        self.item_id: str = item_id
        self.priority: float = priority
        self.value: T = value

    def __repr__(self) -> str:
        return f"HeapNode(id={self.item_id!r}, priority={self.priority}, val={self.value!r})"


class MinHeap(Generic[T]):
    """
    Binary Min-Heap storing HeapNodes.
    Maintains an in-memory index map mapping item_id -> array index for fast O(1) lookups
    and O(log N) random updates/deletions.
    """
    def __init__(self):
        self._heap: list[HeapNode[T]] = []
        self._index_map: dict[str, int] = {}  # item_id -> heap array index

    def __len__(self) -> int:
        return len(self._heap)

    def is_empty(self) -> bool:
        return len(self._heap) == 0

    def contains(self, item_id: str) -> bool:
        """Check if an item exists in the heap in O(1) time."""
        return item_id in self._index_map

    def peek(self) -> Optional[HeapNode[T]]:
        """Return the minimum element without removing it. O(1) time."""
        if not self._heap:
            return None
        return self._heap[0]

    def push(self, item_id: str, priority: float, value: T) -> None:
        """
        Insert a new item into the heap. O(log N) time.
        Raises ValueError if item_id already exists.
        """
        if item_id in self._index_map:
            raise ValueError(f"Item '{item_id}' already exists in heap. Use update_priority instead.")
        
        node = HeapNode(item_id, priority, value)
        idx = len(self._heap)
        self._heap.append(node)
        self._index_map[item_id] = idx
        self._sift_up(idx)

    def pop(self) -> Optional[HeapNode[T]]:
        """
        Remove and return the minimum element. O(log N) time.
        """
        if not self._heap:
            return None
        
        min_node = self._heap[0]
        last_idx = len(self._heap) - 1
        
        if last_idx == 0:
            self._heap.pop()
            del self._index_map[min_node.item_id]
        else:
            self._swap(0, last_idx)
            self._heap.pop()
            del self._index_map[min_node.item_id]
            self._sift_down(0)
            
        return min_node

    def update_priority(self, item_id: str, new_priority: float) -> None:
        """
        Update the priority of an existing item and restore min-heap property. O(log N) time.
        """
        if item_id not in self._index_map:
            raise KeyError(f"Item '{item_id}' not found in heap.")
        
        idx = self._index_map[item_id]
        old_priority = self._heap[idx].priority
        self._heap[idx].priority = new_priority
        
        if new_priority < old_priority:
            self._sift_up(idx)
        elif new_priority > old_priority:
            self._sift_down(idx)

    def decrease_key(self, item_id: str, new_priority: float) -> None:
        """
        Decrease priority of item_id. Raises ValueError if new_priority > current priority.
        O(log N) time.
        """
        if item_id not in self._index_map:
            raise KeyError(f"Item '{item_id}' not found in heap.")
        idx = self._index_map[item_id]
        if new_priority > self._heap[idx].priority:
            raise ValueError(f"new_priority {new_priority} > current priority {self._heap[idx].priority}")
        self.update_priority(item_id, new_priority)

    def remove(self, item_id: str) -> Optional[HeapNode[T]]:
        """
        Remove an arbitrary item from the heap by item_id. O(log N) time.
        """
        if item_id not in self._index_map:
            return None
        
        idx = self._index_map[item_id]
        last_idx = len(self._heap) - 1
        target_node = self._heap[idx]

        if idx == last_idx:
            self._heap.pop()
            del self._index_map[item_id]
        else:
            self._swap(idx, last_idx)
            self._heap.pop()
            del self._index_map[item_id]
            
            # Sift the element swapped into idx to maintain heap order
            if idx < len(self._heap):
                parent_idx = (idx - 1) // 2
                if idx > 0 and self._heap[idx].priority < self._heap[parent_idx].priority:
                    self._sift_up(idx)
                else:
                    self._sift_down(idx)

        return target_node

    def _sift_up(self, idx: int) -> None:
        """Move item at idx up until min-heap property holds."""
        while idx > 0:
            parent_idx = (idx - 1) // 2
            if self._heap[idx].priority < self._heap[parent_idx].priority:
                self._swap(idx, parent_idx)
                idx = parent_idx
            else:
                break

    def _sift_down(self, idx: int) -> None:
        """Move item at idx down until min-heap property holds."""
        n = len(self._heap)
        while True:
            left_child = 2 * idx + 1
            right_child = 2 * idx + 2
            smallest = idx

            if left_child < n and self._heap[left_child].priority < self._heap[smallest].priority:
                smallest = left_child
            if right_child < n and self._heap[right_child].priority < self._heap[smallest].priority:
                smallest = right_child

            if smallest != idx:
                self._swap(idx, smallest)
                idx = smallest
            else:
                break

    def _swap(self, i: int, j: int) -> None:
        """Swap two elements in the heap array and update their indices in index_map."""
        node_i = self._heap[i]
        node_j = self._heap[j]
        self._heap[i], self._heap[j] = node_j, node_i
        self._index_map[node_i.item_id] = j
        self._index_map[node_j.item_id] = i
