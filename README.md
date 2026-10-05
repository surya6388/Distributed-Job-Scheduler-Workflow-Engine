# Distributed Job Scheduler & Workflow Engine

A high-performance **Distributed Job Scheduler & Workflow Engine** designed from scratch without external scheduling frameworks, ORMs, or queue brokers (No Celery, Airflow, Redis, RabbitMQ, networkx, or APScheduler). 

All core data structures, priority queues, DAG topological execution, worker lease managers, rate limiters, and exponential backoff algorithms are implemented directly using Python standard library primitives.

---

## 🌐 Live Website Demo & Code

- 🚀 **Live Interactive Demo**: [https://surya6388.github.io/Distributed-Job-Scheduler-Workflow-Engine/](https://surya6388.github.io/Distributed-Job-Scheduler-Workflow-Engine/)
- 💻 **GitHub Repository**: [https://github.com/surya6388/Distributed-Job-Scheduler-Workflow-Engine](https://github.com/surya6388/Distributed-Job-Scheduler-Workflow-Engine)

---

## Core System Highlights

- **Custom Min-Heap Ready Queue**: Custom binary min-heap maintaining an in-memory index map for $O(1)$ key lookups and $O(\log N)$ decrease-key priority updates.
- **DAG Execution Engine**: Adjacency list graph representation with 3-Color DFS cycle detection, Kahn's algorithm for topological in-degree task unlocking, and Dynamic Programming critical-path SLA analysis.
- **Durable Write-Ahead Log (WAL)**: SQLite WAL mode / PostgreSQL raw SQL persistence. Memory structures rebuild seamlessly from the database on startup.
- **Commit-Then-Apply Consistency**: State transitions commit to durable database transactions *first* before in-memory heaps mutate, guaranteeing resilience against process crashes.
- **Timed Lease Protocol & Cryptographic Fencing**: Prevents double-claiming and late heartbeat race conditions using `lease_token` cryptographic tokens.
- **LRU Cache & Rate Limiters**: Doubly-linked list + hash map LRU cache with TTL eviction for the Dead-Letter Queue (DLQ), accompanied by Token Bucket and Sliding-Window Deque rate limiters.

---

## Data Structure Complexity Matrix

| Structure | File Path | Time Complexity | Space Complexity | Engineering Justification |
| :--- | :--- | :--- | :--- | :--- |
| **Binary Min-Heap with Index Map** | [`src/dsa/min_heap.py`](src/dsa/min_heap.py) | Push: $O(\log N)$<br>Pop: $O(\log N)$<br>Decrease-Key: $O(\log N)$<br>Peek: $O(1)$ | $O(N)$ | Standard Python `heapq` does not support $O(\log N)$ decrease-key or random node deletion. Array + index map enables $O(1)$ lookup and $O(\log N)$ priority updates. |
| **LRU Cache (DLQ & Idempotency)** | [`src/dsa/lru_cache.py`](src/dsa/lru_cache.py) | Get: $O(1)$<br>Put: $O(1)$<br>Evict: $O(1)$ | $O(N)$ | Doubly-linked list (`ListNode`) with sentinel head/tail pointers + hash map. Enables $O(1)$ eviction and TTL expiry without external caching like Redis. |
| **Token Bucket & Sliding Window** | [`src/dsa/rate_limiter.py`](src/dsa/rate_limiter.py) | Token Bucket: $O(1)$<br>Sliding Window: $O(1)$ amortized | Token Bucket: $O(1)$<br>Sliding Window: $O(M)$ | Smooth continuous rate refills per tenant/workflow; sliding-window deque prevents edge-burst abuse. |
| **DAG Topology Engine** | [`src/dsa/dag.py`](src/dsa/dag.py) | 3-Color DFS: $O(V + E)$<br>Kahn Topo Sort: $O(V + E)$<br>DP Critical Path: $O(V + E)$ | $O(V + E)$ | 3-Color DFS detects back-edges; Kahn's algorithm maintains in-degree counts for ready-task unlocking; DP calculates bottleneck critical path lengths. |
| **Lease Expiry Manager** | [`src/scheduler/lease_manager.py`](src/scheduler/lease_manager.py) | Acquire: $O(\log N)$<br>Heartbeat: $O(\log N)$<br>Reclaim: $O(K \log N)$ | $O(N)$ | Expiry Min-Heap ensures $O(1)$ min-expiry check so worker timeout polling never requires an $O(N)$ linear scan over active leases. |

---

## Measured Benchmark & Resilience Metrics

- **Throughput**: `14,285.7 tasks/sec`
- **p95 Dispatch Latency**: `0.007 ms`
- **Cold-Start DB WAL Recovery Time**: `32.4 ms`
- **Chaos Worker Process Crash Test**: `0 Lost Tasks, 0 Duplicate Successful Commits`

---

## Quick Start & Testing

### 1. Installation
```bash
git clone https://github.com/surya6388/Distributed-Job-Scheduler-Workflow-Engine.git
cd Distributed-Job-Scheduler-Workflow-Engine

pip install -r requirements.txt
```

### 2. Run Test Suite
```bash
python -m pytest
```

Expected Output:
```text
======================== 26 passed in 0.69s ========================
```

### 3. Launch REST & WebSocket Server
```bash
uvicorn src.api.server:app --reload
```

---

## Directory Structure

```text
.
├── ARCHITECTURE.md                  # Comprehensive Architecture & Protocol Specifications
├── README.md                        # Documentation & Live Demo Link
├── index.html                       # Standalone Dashboard Page for GitHub Pages Live Hosting
├── pytest.ini                       # Test Runner Configuration
├── requirements.txt                 # Dependencies
├── src/
│   ├── dsa/
│   │   ├── min_heap.py              # Binary Min-Heap with O(1) Index Map & Decrease-Key
│   │   ├── lru_cache.py             # Doubly-Linked List + Hash Map LRU Cache with TTL
│   │   ├── rate_limiter.py          # Token Bucket & Sliding-Window Rate Limiters
│   │   └── dag.py                   # Adjacency List DAG, 3-Color DFS Cycle Check, Kahn Topo Sort, DP Critical Path
│   ├── storage/
│   │   ├── db.py                    # SQLite WAL Raw SQL Manager with Analytical Window Queries
│   │   └── recovery.py              # Commit-Then-Apply Engine & Cold-Start Rebuild
│   ├── scheduler/
│   │   ├── lease_manager.py         # Expiry Min-Heap Lease Manager & Dead-Worker Reclamation
│   │   ├── worker_assignment.py     # Round-Robin vs Least-Loaded Worker Selection
│   │   ├── cron_parser.py           # Standard 5-Field Cron Expression Parser
│   │   └── scheduler_engine.py      # Core In-Memory Hub, Delayed Min-Heap & Exponential Backoff
│   ├── api/
│   │   └── server.py                # FastAPI REST & WebSocket Telemetry Gateway
│   ├── dashboard/
│   │   └── index.html               # React Dashboard with Hand-Drawn SVG DAG Graph
│   ├── chaos/
│   │   └── chaos_monkey.py          # Worker Failure Injection Engine
│   └── benchmark/
│       └── benchmark_engine.py      # Scaling Performance Analyzer (10,000+ Tasks)
└── tests/                           # Complete Pytest Suite (26 Tests)
```

---

## License

Distributed under the [MIT License](LICENSE).
