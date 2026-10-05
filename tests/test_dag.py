import pytest
from dsa.dag import DAGEngine, DAGNode

def test_dag_topological_sort_and_kahn():
    dag = DAGEngine()
    node_a = DAGNode("A", "Fetch Data", cost_weight=2)
    node_b = DAGNode("B", "Process Part 1", cost_weight=5)
    node_c = DAGNode("C", "Process Part 2", cost_weight=3)
    node_d = DAGNode("D", "Aggregate Output", cost_weight=4)

    for n in [node_a, node_b, node_c, node_d]:
        dag.add_node(n)

    dag.add_edge("A", "B")
    dag.add_edge("A", "C")
    dag.add_edge("B", "D")
    dag.add_edge("C", "D")

    assert dag.detect_cycle_dfs() is False
    order = dag.topological_sort_kahn()
    
    assert order[0] == "A"
    assert order[-1] == "D"
    assert set(order[1:3]) == {"B", "C"}

def test_dag_cycle_detection_dfs():
    dag = DAGEngine()
    n1 = DAGNode("1", "T1")
    n2 = DAGNode("2", "T2")
    n3 = DAGNode("3", "T3")

    for n in [n1, n2, n3]:
        dag.add_node(n)

    dag.add_edge("1", "2")
    dag.add_edge("2", "3")
    dag.add_edge("3", "1")  # Cycle!

    assert dag.detect_cycle_dfs() is True
    with pytest.raises(ValueError, match="contains a cycle"):
        dag.topological_sort_kahn()

def test_dag_critical_path_dp():
    dag = DAGEngine()
    # Path A(2) -> B(5) -> D(4) = 11
    # Path A(2) -> C(3) -> D(4) = 9
    node_a = DAGNode("A", "Start", cost_weight=2)
    node_b = DAGNode("B", "Long Task", cost_weight=5)
    node_c = DAGNode("C", "Short Task", cost_weight=3)
    node_d = DAGNode("D", "Finish", cost_weight=4)

    for n in [node_a, node_b, node_c, node_d]:
        dag.add_node(n)

    dag.add_edge("A", "B")
    dag.add_edge("A", "C")
    dag.add_edge("B", "D")
    dag.add_edge("C", "D")

    max_cost, critical_path = dag.compute_critical_path_dp()
    assert max_cost == 11
    assert critical_path == ["A", "B", "D"]
