from collections import deque
from typing import Iterable, List, Dict, Set


def sort_by_dependency(items: List[Dict[str, object]]) -> List[Dict[str, object]]:
    """Return items sorted so that dependencies appear before dependents.

    Each item should be a mapping with a ``table`` key and an optional
    ``requires`` key listing dependencies. Table names are compared in a
    case-insensitive manner by normalizing to lower case. Dependencies that
    are not present in ``items`` are ignored.

    Raises
    ------
    ValueError
        If a circular dependency is detected among the provided tables.
    """
    # Map normalized table name to item for lookup
    table_map: Dict[str, Dict[str, object]] = {}
    for item in items:
        table = item["table"].lower()
        norm_item = {**item, "table": table}
        if "requires" in item:
            norm_item["requires"] = [r.lower() for r in item.get("requires", [])]
        table_map[table] = norm_item

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


def build_dependency_graph(settings: Iterable[Dict[str, object]]) -> List[Dict[str, object]]:
    """Return dependency items derived from settings mappings.

    Parameters
    ----------
    settings:
        Iterable of settings dictionaries. Each mapping must include a
        ``dst_table_name`` key and may include ``src_table_name`` to denote
        lineage to another table.

    Returns
    -------
    List[Dict[str, object]]
        A list of ``{"table": dst, "requires": [src]}`` dictionaries suitable
        for :func:`sort_by_dependency`. Dependencies to tables that do not
        appear as a ``dst_table_name`` in ``settings`` are ignored.
    """

    table_map: Dict[str, Dict[str, object]] = {
        s["dst_table_name"].lower(): s for s in settings if s.get("dst_table_name")
    }

    items: List[Dict[str, object]] = []
    for dst, cfg in table_map.items():
        src = cfg.get("src_table_name")
        src_norm = src.lower() if src else None
        requires = [src_norm] if src_norm and src_norm in table_map else []
        items.append({"table": dst, "requires": requires})

    return items
