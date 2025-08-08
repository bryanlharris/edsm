from collections import deque
from typing import List, Dict, Set


def sort_by_dependency(items: List[Dict[str, object]]) -> List[Dict[str, object]]:
    """Return items sorted so that dependencies appear before dependents.

    Each item should be a mapping with a ``table`` key and an optional
    ``requires`` key listing dependencies. Dependencies that are not present
    in ``items`` are ignored.

    Raises
    ------
    ValueError
        If a circular dependency is detected among the provided tables.
    """
    # Map table name to item for lookup
    table_map: Dict[str, Dict[str, object]] = {item["table"]: item for item in items}
    nodes: Set[str] = set(table_map)

    # Build dependency (edges) within provided nodes
    requires_map: Dict[str, Set[str]] = {}
    dependents: Dict[str, Set[str]] = {t: set() for t in nodes}
    for table, item in table_map.items():
        deps = set(item.get("requires", [])) & nodes
        requires_map[table] = deps
        for dep in deps:
            dependents.setdefault(dep, set()).add(table)

    indegree: Dict[str, int] = {t: len(requires_map[t]) for t in nodes}
    queue = deque([t for t, d in indegree.items() if d == 0])
    ordered: List[str] = []

    while queue:
        node = queue.popleft()
        ordered.append(node)
        for dep in dependents.get(node, set()):
            indegree[dep] -= 1
            if indegree[dep] == 0:
                queue.append(dep)

    if len(ordered) != len(nodes):
        remaining = nodes - set(ordered)
        raise ValueError(f"Circular dependency detected: {', '.join(sorted(remaining))}")

    return [table_map[name] for name in ordered]
