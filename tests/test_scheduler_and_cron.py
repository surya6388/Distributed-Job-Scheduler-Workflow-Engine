import datetime
import pytest
from scheduler.cron_parser import CronParser
from scheduler.scheduler_engine import SchedulerEngine, TaskInstance

def test_cron_parser_next_run():
    # Every day at 04:30 AM ("30 4 * * *")
    parser = CronParser("30 4 * * *")
    dt = datetime.datetime(2026, 10, 6, 4, 0, 0)
    next_dt = parser.compute_next_run(dt)
    
    assert next_dt == datetime.datetime(2026, 10, 6, 4, 30, 0)

    # Test step values ("*/15 * * * *")
    parser_step = CronParser("*/15 * * * *")
    dt_step = datetime.datetime(2026, 10, 6, 4, 10, 0)
    next_step = parser_step.compute_next_run(dt_step)
    assert next_step == datetime.datetime(2026, 10, 6, 4, 15, 0)

def test_scheduler_retries_and_dlq():
    engine = SchedulerEngine(dlq_capacity=10)
    task = TaskInstance("task-test", "run-1", "node-1", priority=1, max_retries=2, retry_delay_sec=10)
    engine.register_task(task)

    now = 1000.0

    # Failure 1 -> Attempt 1 -> RETRYING state
    status1 = engine.handle_task_failure("task-test", "Connection timeout", now=now)
    assert status1 == 'RETRYING'
    assert task.attempt == 1

    # At t=1002, task should still be in delayed queue
    due_0 = engine.poll_due_retries(now=1002.0)
    assert len(due_0) == 0

    # At t=1025, delayed retry fires -> task returned to READY
    due_1 = engine.poll_due_retries(now=1025.0)
    assert len(due_1) == 1
    assert task.state == 'READY'

    # Failure 2 -> Attempt 2 -> RETRYING
    status2 = engine.handle_task_failure("task-test", "Connection timeout", now=1026.0)
    assert status2 == 'RETRYING'

    # Failure 3 -> Attempt 3 > max_retries(2) -> Moves to DEAD & DLQ
    status3 = engine.handle_task_failure("task-test", "Out of memory", now=1050.0)
    assert status3 == 'DEAD'
    assert engine.dlq_store.contains("task-test")
    dlq_record = engine.dlq_store.get("task-test")
    assert dlq_record["reason"] == "Out of memory"
