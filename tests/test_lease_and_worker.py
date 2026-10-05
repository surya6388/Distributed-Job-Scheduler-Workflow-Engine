import pytest
from scheduler.lease_manager import LeaseManager
from scheduler.worker_assignment import RoundRobinAssignment, LeastLoadedAssignment

def test_lease_acquisition_and_heartbeat():
    lm = LeaseManager()
    now = 1000.0

    token = lm.acquire_lease("task-1", "worker-1", timeout_sec=10.0, now=now)
    assert token is not None
    assert lm.lease_map["task-1"][0] == "worker-1"

    # Renew heartbeat after 5s with correct token -> Success
    success = lm.renew_heartbeat("task-1", token, extension_sec=15.0, now=now + 5.0)
    assert success is True

    # Renew with invalid token -> Rejected
    fail_token = lm.renew_heartbeat("task-1", "bad-token", extension_sec=10.0, now=now + 6.0)
    assert fail_token is False

def test_lease_expiry_and_dead_worker_reclamation():
    lm = LeaseManager()
    now = 1000.0

    lm.acquire_lease("t1", "w1", timeout_sec=5.0, now=now)   # expires t=1005
    lm.acquire_lease("t2", "w2", timeout_sec=15.0, now=now)  # expires t=1015

    # At t=1002, no leases expired
    reclaimed = lm.reclaim_expired_leases(now=1002.0)
    assert reclaimed == []

    # At t=1006, worker 1's lease on t1 has expired
    reclaimed = lm.reclaim_expired_leases(now=1006.0)
    assert reclaimed == ["t1"]
    assert "t1" not in lm.lease_map

def test_worker_assignment_strategies():
    workers = ["w1", "w2", "w3"]
    
    # Test Round-Robin
    rr = RoundRobinAssignment(workers)
    assert [rr.select_worker() for _ in range(5)] == ["w1", "w2", "w3", "w1", "w2"]

    # Test Least-Loaded Min-Heap
    ll = LeastLoadedAssignment(workers)
    # Assign 3 tasks -> w1, w2, w3 each get 1
    w_a = ll.select_worker()
    w_b = ll.select_worker()
    w_c = ll.select_worker()
    assert set([w_a, w_b, w_c]) == {"w1", "w2", "w3"}

    # Next task -> should pick one with load 1
    w_d = ll.select_worker()
    # Release w_a -> load becomes 0
    ll.release_worker(w_a)
    # Next selection MUST pick w_a (load 0)
    assert ll.select_worker() == w_a
