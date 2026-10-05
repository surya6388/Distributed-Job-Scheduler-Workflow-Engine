import pytest
from dsa.min_heap import MinHeap

def test_push_and_pop_ordering():
    heap = MinHeap()
    heap.push("task-3", 30, "p3")
    heap.push("task-1", 10, "p1")
    heap.push("task-2", 20, "p2")
    heap.push("task-5", 50, "p5")
    heap.push("task-4", 40, "p4")

    assert len(heap) == 5
    assert heap.peek().item_id == "task-1"

    nodes = []
    while not heap.is_empty():
        nodes.append(heap.pop())

    priorities = [n.priority for n in nodes]
    assert priorities == [10, 20, 30, 40, 50]
    assert len(heap) == 0

def test_decrease_key_and_update_priority():
    heap = MinHeap()
    heap.push("t1", 100, "val1")
    heap.push("t2", 200, "val2")
    heap.push("t3", 300, "val3")

    # Decrease priority of t3 to 50 -> should become top of min heap
    heap.decrease_key("t3", 50)
    assert heap.peek().item_id == "t3"
    assert heap.peek().priority == 50

    # Increase priority of t3 to 500 -> t1 should become top
    heap.update_priority("t3", 500)
    assert heap.peek().item_id == "t1"

    top = heap.pop()
    assert top.item_id == "t1"
    assert top.priority == 100

def test_remove_arbitrary_node():
    heap = MinHeap()
    heap.push("a", 10, "A")
    heap.push("b", 20, "B")
    heap.push("c", 30, "C")
    heap.push("d", 40, "D")

    # Remove element 'b' in middle
    removed = heap.remove("b")
    assert removed.item_id == "b"
    assert not heap.contains("b")
    assert len(heap) == 3

    popped = []
    while not heap.is_empty():
        popped.append(heap.pop().item_id)
    assert popped == ["a", "c", "d"]

def test_duplicate_push_raises_error():
    heap = MinHeap()
    heap.push("task-1", 10, "v1")
    with pytest.raises(ValueError, match="already exists"):
        heap.push("task-1", 5, "v1_dup")

def test_empty_heap_operations():
    heap = MinHeap()
    assert heap.peek() is None
    assert heap.pop() is None
    assert heap.remove("nonexistent") is None
