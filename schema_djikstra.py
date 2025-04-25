from table_entities import ForeignKeys, Table
from dataclasses import dataclass, field
from langchain_core.tools import tool


@dataclass
class DjikstraNodes:
    # Here we have the reference of the current table
    # and what it is joining
    data: ForeignKeys | None = None
    is_reference: bool = False
    weight: int = -1
    node_name: str | None = None


@dataclass
class Djikstra:
    # The list of fully explored nodes
    history: dict[str, bool] = field(default_factory=dict)
    # Adjecency list, the graph perse
    graph: dict[str, Table] = field(default_factory=dict)
    # The djikstra nodes, this contains the shortest path
    shortest_path: dict[str, DjikstraNodes] = field(default_factory=dict)


@tool
def create_djikstra(tables: list[Table], tables_needed: list[str]) -> dict[str, DjikstraNodes]:
    """
    Tool that makes a djikstra seacth on the list of tables, it returns the shortest path
    and the connections
    """
    _graph: dict[str, Table] = {}
    _history: dict[str, bool] = {}
    _shortest_path: dict[str, DjikstraNodes | None] = {}

    for table in tables:
        _shortest_path[table.table_name] = DjikstraNodes()
        _history[table.table_name] = False
        _graph[table.table_name] = table

    if (len(tables_needed) < 1):
        return "To use this tool you require to give a list of, at least, 2 tables"

    initial_table = tables_needed[0]
    _shortest_path[initial_table] = DjikstraNodes(
        node_name=initial_table,
        data=None,
        weight=0
    )

    djikstra = Djikstra(
        shortest_path=_shortest_path,
        history=_history,
        graph=_graph,
    )

    build_djikstra(djikstra, djikstra.shortest_path[initial_table])
    return djikstra.shortest_path


def build_djikstra(djikstra: Djikstra, act_node: DjikstraNodes):
    nodes: list[DjikstraNodes] = []
    current_distance = act_node.weight
    for _, fks in djikstra.graph[act_node.node_name].foreign_keys.items():
        for fk in fks:
            if djikstra.history[fk.reference_table]:
                continue

            nodes.append(DjikstraNodes(
                data=fk,
                is_reference=False,
                weight=current_distance + 1,
                node_name=fk.reference_table,
            ))

    for _, fks in djikstra.graph[act_node.node_name].references_to_table.items():
        for fk in fks:
            if djikstra.history[fk.referencing_table]:
                continue

            nodes.append(DjikstraNodes(
                data=fk,
                is_reference=True,
                weight=current_distance + 1,
                node_name=fk.referencing_table,
            ))

    djikstra.history[act_node.node_name] = True

    for node in nodes:
        if (djikstra.shortest_path[node.node_name].weight > node.weight):
            djikstra.shortest_path[node.node_name] = node

        build_djikstra(djikstra, node)
