"""
Rate Limiter Implementations: Token Bucket & Sliding-Window Deque.

Time Complexity:
- TokenBucketRateLimiter.allow_request: O(1)
- SlidingWindowRateLimiter.allow_request: O(1) amortized

Space Complexity:
- Token Bucket: O(1)
- Sliding Window: O(M) where M is max_requests in window
"""

import time
from collections import deque
from typing import Optional

class TokenBucketRateLimiter:
    """
    Token Bucket Rate Limiter per tenant / workflow.
    Tokens refill continuously at refill_rate (tokens per second) up to capacity.
    """
    def __init__(self, capacity: float, refill_rate: float, initial_time: Optional[float] = None):
        if capacity <= 0 or refill_rate <= 0:
            raise ValueError("Capacity and refill_rate must be positive.")
        self.capacity: float = float(capacity)
        self.refill_rate: float = float(refill_rate)
        self.tokens: float = float(capacity)
        self.last_refill_time: float = initial_time if initial_time is not None else time.time()

    def _refill(self, now: float) -> None:
        if now < self.last_refill_time:
            self.last_refill_time = now
            return
        elapsed = now - self.last_refill_time
        if elapsed > 0:
            added_tokens = elapsed * self.refill_rate
            self.tokens = min(self.capacity, self.tokens + added_tokens)
            self.last_refill_time = now

    def allow_request(self, tokens_requested: float = 1.0, now: Optional[float] = None) -> bool:
        """
        Check and consume tokens if available. Returns True if permitted, False otherwise. O(1).
        """
        current_time = now if now is not None else time.time()
        self._refill(current_time)

        if self.tokens >= tokens_requested:
            self.tokens -= tokens_requested
            return True
        return False

    def available_tokens(self, now: Optional[float] = None) -> float:
        current_time = now if now is not None else time.time()
        self._refill(current_time)
        return self.tokens


class SlidingWindowRateLimiter:
    """
    Sliding Window Rate Limiter using a timestamp Deque.
    Evicts timestamps older than window_size_sec.
    """
    def __init__(self, max_requests: int, window_size_sec: float):
        if max_requests <= 0 or window_size_sec <= 0:
            raise ValueError("max_requests and window_size_sec must be positive.")
        self.max_requests: int = max_requests
        self.window_size_sec: float = float(window_size_sec)
        self.timestamps: deque[float] = deque()

    def allow_request(self, now: Optional[float] = None) -> bool:
        """
        Evict expired timestamps and record current request if within limit. O(1) amortized.
        """
        current_time = now if now is not None else time.time()
        cutoff = current_time - self.window_size_sec

        # Remove timestamps outside the sliding window
        while self.timestamps and self.timestamps[0] <= cutoff:
            self.timestamps.popleft()

        if len(self.timestamps) < self.max_requests:
            self.timestamps.append(current_time)
            return True
        return False

    def current_count(self, now: Optional[float] = None) -> int:
        current_time = now if now is not None else time.time()
        cutoff = current_time - self.window_size_sec
        while self.timestamps and self.timestamps[0] <= cutoff:
            self.timestamps.popleft()
        return len(self.timestamps)
