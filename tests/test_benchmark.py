import pytest
from storage.db import DatabaseManager
from benchmark.benchmark_engine import SchedulerBenchmark

def test_high_scale_benchmark_throughput():
    db = DatabaseManager(":memory:")
    bench = SchedulerBenchmark(db)

    results = bench.run_benchmark(num_tasks=1000)

    assert results["num_tasks"] == 1000
    assert results["throughput_tasks_per_sec"] > 500
    assert results["p95_latency_ms"] < 10.0
    assert results["recovery_time_ms"] >= 0.0
