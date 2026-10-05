import time
import pytest
from dsa.lru_cache import LRUCache

def test_lru_basic_get_put():
    cache = LRUCache(capacity=3)
    cache.put("a", 1)
    cache.put("b", 2)
    cache.put("c", 3)

    assert cache.get("a") == 1
    assert cache.get("b") == 2
    assert cache.get("c") == 3
    assert len(cache) == 3

def test_lru_eviction():
    cache = LRUCache(capacity=3)
    cache.put("a", 1)
    cache.put("b", 2)
    cache.put("c", 3)

    # Access "a" so order becomes: b (LRU), c, a (MRU)
    cache.get("a")

    # Put "d" -> "b" should be evicted
    cache.put("d", 4)
    assert cache.get("b") is None
    assert cache.get("a") == 1
    assert cache.get("c") == 3
    assert cache.get("d") == 4

def test_lru_ttl_expiration():
    cache = LRUCache(capacity=5)
    cache.put("k1", "v1", ttl_sec=0.1)
    cache.put("k2", "v2", ttl_sec=10.0)

    assert cache.get("k1") == "v1"
    time.sleep(0.15)

    # k1 should be expired
    assert cache.get("k1") is None
    assert cache.get("k2") == "v2"

def test_lru_remove_and_pop_lru():
    cache = LRUCache(capacity=3)
    cache.put("x", 100)
    cache.put("y", 200)

    assert cache.remove("x") is True
    assert cache.get("x") is None
    assert len(cache) == 1

    kv = cache.pop_lru()
    assert kv == ("y", 200)
    assert len(cache) == 0
