import time
import pytest
from dsa.rate_limiter import TokenBucketRateLimiter, SlidingWindowRateLimiter

def test_token_bucket_rate_limiter():
    now = 1000.0
    limiter = TokenBucketRateLimiter(capacity=2, refill_rate=10.0, initial_time=now) # 10 tokens/sec

    assert limiter.allow_request(1.0, now=now) is True
    assert limiter.allow_request(1.0, now=now) is True
    # Bucket now empty
    assert limiter.allow_request(1.0, now=now) is False

    # After 0.1s, 1 token refilled (0.1 * 10 = 1)
    now += 0.1
    assert limiter.allow_request(1.0, now=now) is True
    assert limiter.allow_request(1.0, now=now) is False

def test_sliding_window_rate_limiter():
    limiter = SlidingWindowRateLimiter(max_requests=2, window_size_sec=5.0)
    now = 100.0

    assert limiter.allow_request(now=now) is True
    assert limiter.allow_request(now=now + 1.0) is True
    # 3rd request within 5s window rejected
    assert limiter.allow_request(now=now + 2.0) is False

    # At t=105.1s, the request at t=100.0 is evicted
    assert limiter.allow_request(now=now + 5.1) is True
