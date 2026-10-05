"""
Hand-Built Directed Acyclic Graph (DAG) Engine.

Algorithmic Core:
1. Cycle Detection: DFS 3-Coloring (WHITE=0, GRAY=1, BLACK=2) -> O(V + E)
2. Topological Sort: Kahn's Algorithm using In-Degree Map -> O(V + E)
3. Critical-Path Length: Dynamic Programming over Topological Order -> O(V + E)

Time Complexity:
- add_node / add_edge: O(1)
- detect_cycle: O(V + E)
- topological_sort: O(V + E)
- compute_critical_path: O(V + E)

Space Complexity:
- O(V + E) for adjacency list, in-degree map, and DP tables.
"""

from typing import Dict, List, Set, Tuple, Optional

class DAGNode:
    __slots__ = ('node_id', 'task_name', 'cost_weight', 'max_retries', 'retry_delay_sec', 'timeout_sec')

    def __init__(
        self,
        node_id: str,
        task_name: str,
        cost_weight: int = 1,
        max_retries: int = 3,
        retry_delay_sec: int = 5,
        timeout_sec: int = 30
    ):
        self.node_id: str = node_id
        self.task_name: str = task_name
        self.cost_weight: int = max(1, cost_weight)
        self.max_retries: int = max_retries
        self.retry_delay_sec: int = retry_delay_sec
        self.timeout_sec: int = timeout_sec

    def __repr__(self) -> str:
        return f"DAGNode(id={self.node_id!r}, name={self.task_name!r}, cost={self.cost_weight})"


class DAGEngine:
    """
    DAG Execution Engine storing nodes and dependencies via Adjacency Lists.
    Maintains in-degree counts for Kahn's topological evaluation.
    """
    # 3-Color DFS Constants
    COLOR_WHITE = 0  # Unvisited
    COLOR_GRAY = 1   # Visiting (in current recursion stack)
    COLOR_BLACK = 2  # Visited

    def __init__(self):
        self.nodes: Dict[str, DAGNode] = {}
        self.adj_list: Dict[str, List[str]] = {}       # parent_id -> list of child_ids
        self.in_adj_list: Dict[str, List[str]] = {}    # child_id -> list of parent_ids
        self.in_degree: Dict[str, int] = {}            # node_id -> in-degree count

    def add_node(self, node: DAGNode) -> None:
        """Add a node to the DAG. O(1)."""
        if node.node_id in self.nodes:
            raise ValueError(f"Node '{node.node_id}' already exists in DAG.")
        self.nodes[node.node_id] = node
        self.adj_list[node.node_id] = []
        self.in_adj_list[node.node_id] = []
        self.in_degree[node.node_id] = 0

    def add_edge(self, parent_id: str, child_id: str) -> None:
        """
        Add a directed dependency edge (parent_id -> child_id).
        child_id depends on parent_id completing first. O(1).
        """
        if parent_id not in self.nodes:
            raise KeyError(f"Parent node '{parent_id}' not found in DAG.")
        if child_id not in self.nodes:
            raise KeyError(f"Child node '{child_id}' not found in DAG.")
        if parent_id == child_id:
            raise ValueError(f"Self-loop detected on node '{parent_id}'.")

        if child_id not in self.adj_list[parent_id]:
            self.adj_list[parent_id].append(child_id)
            self.in_adj_list[child_id].append(parent_id)
            self.in_degree[child_id] += 1

    def detect_cycle_dfs(self) -> bool:
        """
        3-Color Depth First Search (DFS) Cycle Detection.
        Returns True if a cycle exists, False if valid DAG. O(V + E) time.
        """
        color: Dict[str, int] = {node_id: self.COLOR_WHITE for node_id in self.nodes}

        def _dfs_visit(u: str) -> bool:
            color[u] = self.COLOR_GRAY
            for v in self.adj_list[u]:
                if color[v] == self.COLOR_GRAY:
                    return True  # Back-edge detected -> Cycle found!
                if color[v] == self.COLOR_WHITE:
                    if _dfs_visit(v):
                        return True
            color[u] = self.COLOR_BLACK
            return False

        for node_id in self.nodes:
            if color[node_id] == self.COLOR_WHITE:
                if _dfs_visit(node_id):
                    return True
        return False

    def topological_sort_kahn(self) -> List[str]:
        """
        Kahn's Algorithm for Topological Sorting using In-Degree Map.
        Returns valid execution order list of node_ids.
        Raises ValueError if a cycle is present. O(V + E) time.
        """
        in_deg_copy = self.in_degree.copy()
        zero_in_degree = [node_id for node_id, deg in in_deg_copy.items() if deg == 0]
        topo_order: List[str] = []

        while zero_in_degree:
            u = zero_in_degree.pop(0)
            topo_order.append(u)

            for v in self.adj_list[u]:
                in_deg_copy[v] -= 1
                if in_deg_copy[v] == 0:
                    zero_in_degree.append(v)

        if len(topo_order) != len(self.nodes):
            raise ValueError("DAG contains a cycle; topological sort impossible.")

        return topo_order

    def compute_critical_path_dp(self) -> Tuple[int, List[str]]:
        """
        Dynamic Programming calculation of Critical Path (longest path length & path sequence).
        Based on node cost_weight values over topological order. O(V + E) time.
        Returns (max_cost, path_list).
        """
        topo_order = self.topological_sort_kahn()
        dist: Dict[str, int] = {}
        parent_map: Dict[str, Optional[str]] = {}

        for u in topo_order:
            dist[u] = self.nodes[u].cost_weight
            parent_map[u] = None

        for u in topo_order:
            for v in self.adj_list[u]:
                new_cost = dist[u] + self.nodes[v].cost_weight
                if new_cost > dist[v]:
                    dist[v] = new_cost
                    parent_map[v] = u

        if not dist:
            return 0, []

        max_node = max(dist, key=lambda k: dist[k])
        max_cost = dist[max_node]

        # Reconstruct path backwards from max_node to root
        path: List[str] = []
        curr: Optional[str] = max_node
        while curr is not None:
            path.append(curr)
            curr = parent_map[curr]

        path.reverse()
        return max_cost, path
