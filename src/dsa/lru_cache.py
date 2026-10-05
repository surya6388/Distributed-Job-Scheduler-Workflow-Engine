"""
Custom LRU Cache Implementation using Doubly-Linked List + Hash Map.

Time Complexity:
- get: O(1)
- put: O(1)
- remove: O(1)
- pop_lru: O(1)
- evict_expired: O(K) where K is expired elements count

Space Complexity:
- O(N) where N is capacity.
"""

import time
from typing import Any, Generic, TypeVar, Optional, Tuple

K = TypeVar('K')
V = TypeVar('V')

class ListNode(Generic[K, V]):
    __slots__ = ('key', 'value', 'expiry_time', 'prev', 'next')

    def __init__(self, key: Optional[K] = None, value: Optional[V] = None, ttl_sec: Optional[float] = None):
        self.key: Optional[K] = key
        self.value: Optional[V] = value
        self.expiry_time: Optional[float] = (time.time() + ttl_sec) if ttl_sec is not None else None
        self.prev: Optional['ListNode[K, V]'] = None
        self.next: Optional['ListNode[K, V]'] = None

    def is_expired(self, now: Optional[float] = None) -> bool:
        if self.expiry_time is None:
            return False
        current_time = now if now is not None else time.time()
        return current_time >= self.expiry_time


class LRUCache(Generic[K, V]):
    """
    Thread-unsafe LRU Cache with Sentinel Head/Tail pointers and Hash Map.
    Supports TTL expiration for Dead-Letter Queue & Idempotency Store.
    """
    def __init__(self, capacity: int):
        if capacity <= 0:
            raise ValueError("Capacity must be greater than 0.")
        self.capacity: int = capacity
        self.map: dict[K, ListNode[K, V]] = {}

        # Sentinel nodes
        self.head: ListNode[K, V] = ListNode()
        self.tail: ListNode[K, V] = ListNode()
        self.head.next = self.tail
        self.tail.prev = self.head

    def __len__(self) -> int:
        return len(self.map)

    def contains(self, key: K) -> bool:
        if key not in self.map:
            return False
        node = self.map[key]
        if node.is_expired():
            self._remove_node(node)
            del self.map[key]
            return False
        return True

    def get(self, key: K) -> Optional[V]:
        """Get value by key and move node to MRU (most recently used, head). O(1)."""
        if key not in self.map:
            return None

        node = self.map[key]
        if node.is_expired():
            self._remove_node(node)
            del self.map[key]
            return None

        self._move_to_head(node)
        return node.value

    def put(self, key: K, value: V, ttl_sec: Optional[float] = None) -> None:
        """Insert or update key-value pair with optional TTL. O(1)."""
        if key in self.map:
            node = self.map[key]
            node.value = value
            node.expiry_time = (time.time() + ttl_sec) if ttl_sec is not None else None
            self._move_to_head(node)
        else:
            if len(self.map) >= self.capacity:
                self.pop_lru()

            new_node = ListNode(key, value, ttl_sec)
            self.map[key] = new_node
            self._add_to_head(new_node)

    def remove(self, key: K) -> bool:
        """Remove entry by key. O(1)."""
        if key not in self.map:
            return False
        node = self.map[key]
        self._remove_node(node)
        del self.map[key]
        return True

    def pop_lru(self) -> Optional[Tuple[K, V]]:
        """Evict and return the least recently used item (tail.prev). O(1)."""
        if len(self.map) == 0:
            return None
        lru_node = self.tail.prev
        if lru_node is self.head or lru_node.key is None:
            return None

        self._remove_node(lru_node)
        del self.map[lru_node.key]
        return (lru_node.key, lru_node.value)

    def evict_expired(self) -> int:
        """Scan and evict all expired entries. Returns count of evicted keys."""
        now = time.time()
        expired_keys = [k for k, node in self.map.items() if node.is_expired(now)]
        for k in expired_keys:
            self.remove(k)
        return len(expired_keys)

    # --- Private Helper Methods ---

    def _add_to_head(self, node: ListNode[K, V]) -> None:
        """Insert node right after head sentinel (MRU position)."""
        node.prev = self.head
        node.next = self.head.next
        self.head.next.prev = node
        self.head.next = node

    def _remove_node(self, node: ListNode[K, V]) -> None:
        """Unlink node from doubly-linked list."""
        prev_node = node.prev
        next_node = node.next
        if prev_node:
            prev_node.next = next_node
        if next_node:
            next_node.prev = prev_node

    def _move_to_head(self, node: ListNode[K, V]) -> None:
        """Move an existing node to head (MRU)."""
        self._remove_node(node)
        self._add_to_head(node)
