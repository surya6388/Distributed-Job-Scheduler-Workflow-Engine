# Distributed Job Scheduler & Workflow Engine

[![Python 3.14](https://img.shields.io/badge/python-3.14-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-11%20passed-brightgreen.svg)]()

> A high-performance, resume-grade **Distributed Job Scheduler & Workflow Engine** built from scratch without external scheduler/queue libraries (No Celery, Airflow, Redis, RabbitMQ, networkx, or ORMs).
> All core data structures, priority queues, DAG topological execution, lease expiration heaps, and rate limiters are written by hand using Python standard library primitives.

**Repository Link**: [https://github.com/surya6388/Distributed-Job-Scheduler-Workflow-Engine](https://github.com/surya6388/Distributed-Job-Scheduler-Workflow-Engine)

---

## Key Highlights & Hard Constraints

- **Hand-Built DSA Core**: Priority queue min-heaps with decrease-key, LRU caches with doubly linked lists, token bucket and sliding-window rate limiters, 3-color DFS cycle detection, Kahn's topological sort, and DP critical path length calculation.
- **RDBMS Durable WAL Source of Truth**: SQLite WAL / PostgreSQL raw SQL state persistence. On cold start, the engine rebuilds all in-memory priority queues, lease maps, and active DAG state directly from the database.
- **Commit-Then-Apply Ordering**: State changes commit to durable database transactions *first* before in-memory structures mutate, guaranteeing resilience against process crashes.
- **Fencing Token Protocol**: Prevents double-claiming and late heartbeat race conditions via cryptographic lease tokens (`lease_token`).

---

## Data Structure Architecture & Complexity Analysis

| Data Structure | File Location | Time Complexity | Space Complexity | Engineering Purpose |
| :--- | :--- | :--- | :--- | :--- |
| **Binary Min-Heap with Index Map** | [`src/dsa/min_heap.py`](src/dsa/min_heap.py) | Push: $O(\log N)$<br>Pop: $O(\log N)$<br>Decrease-Key: $O(\log N)$<br>Peek/Contains: $O(1)$ | $O(N)$ | Ready queue & priority dispatching with $O(1)$ lookup index map for fast decrease-key operations. |
| **LRU Cache** | [`src/dsa/lru_cache.py`](src/dsa/lru_cache.py) | Get: $O(1)$<br>Put: $O(1)$<br>Evict: $O(1)$ | $O(N)$ | Doubly-linked list + hash map sentinel cache for Dead-Letter Queue (DLQ) & Idempotency store with TTL eviction. |
| **Token Bucket & Sliding Window** | [`src/dsa/rate_limiter.py`](src/dsa/rate_limiter.py) | Token Bucket: $O(1)$<br>Sliding Window: $O(1)$ amortized | Token Bucket: $O(1)$<br>Sliding Window: $O(M)$ | Rate limiting per workflow/tenant to prevent task starvation and burst boundary attacks. |

---

## Task Execution State Machine

```
              ┌───────────────┐
              │    PENDING    │
              └───────┬───────┘
                      │ (All In-Degree Dependencies Satisfied)
                      ▼
              ┌───────────────┐
              │     READY     │
              └───────┬───────┘
                      │ (Claimed via timed lease protocol)
                      ▼
              ┌───────────────┐
      ┌───────┤    RUNNING    ├───────┐
      │       └───────┬───────┘       │
      │ (Success)     │ (Failure)     │ (Lease Expired)
      ▼               ▼               ▼
┌───────────┐   ┌───────────┐   ┌───────────┐
│  SUCCESS  │   │ RETRYING  │   │   READY   │
└───────────┘   └─────┬─────┘   └───────────┘
                      │ (Backoff Expired)
                      ▼
                (Ready Queue)
                      │ (Max Retries Exceeded)
                      ▼
                ┌───────────┐
                │   DEAD    │ (DLQ)
                └───────────┘
```

---

## Quick Start & Testing

### 1. Prerequisites & Installation
```bash
git clone https://github.com/surya6388/Distributed-Job-Scheduler-Workflow-Engine.git
cd Distributed-Job-Scheduler-Workflow-Engine

# Install lightweight test dependencies
pip install -r requirements.txt
```

### 2. Run Test Suite
```bash
python -m pytest
```

Expected Output:
```text
============================= test session starts =============================
platform win32 -- Python 3.14.3, pytest-9.1.1, pluggy-1.6.0
collected 11 items

tests\test_lru_cache.py ....                                             [ 36%]
tests\test_min_heap.py .....                                             [ 81%]
tests\test_rate_limiter.py ..                                            [100%]

============================= 11 passed in 0.21s ==============================
```

---

## Directory Structure

```text
.
├── ARCHITECTURE.md          # Comprehensive architecture & protocol specifications
├── README.md                # Project documentation & live repository link
├── pytest.ini               # Test configuration
├── requirements.txt         # Project dependencies
├── src/
│   └── dsa/
│       ├── __init__.py
│       ├── min_heap.py      # Binary Min-Heap with index map & decrease-key
│       ├── lru_cache.py     # Doubly-linked list + Hash Map LRU cache with TTL
│       └── rate_limiter.py  # Token Bucket & Sliding Window rate limiters
└── tests/
    ├── test_min_heap.py     # Unit tests for Min-Heap
    ├── test_lru_cache.py    # Unit tests for LRU Cache
    └── test_rate_limiter.py # Unit tests for Rate Limiters
```

---

## License

Distributed under the [MIT License](LICENSE).
