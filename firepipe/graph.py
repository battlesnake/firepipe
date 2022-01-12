from typing import List, Set, Dict, TypeVar, Generic
from collections import deque


class Node():
    """ Represents a node in a graph """


NodeType = TypeVar('NodeType', bound=Node)


class Graph(Generic[NodeType]):
    """ Represents a directed graph """

    nodes : Set[NodeType]
    outgoing_edges: Dict[NodeType, Set[NodeType]]
    incoming_edges: Dict[NodeType, Set[NodeType]]

    def __init__(self):
        self.nodes = set()
        self.outgoing_edges = dict()
        self.incoming_edges = dict()

    def add_node(self, node: NodeType) -> None:
        self.nodes.add(node)
        if not node in self.outgoing_edges:
            self.outgoing_edges[node] = set()
        if not node in self.incoming_edges:
            self.incoming_edges[node] = set()

    def add_edge(self, source: NodeType, target: NodeType) -> None:
        # Forward
        if source not in self.nodes:
            self.add_node(source)
        self.outgoing_edges[source].add(target)
        # Backward
        if target not in self.nodes:
            self.add_node(target)
        self.incoming_edges[target].add(source)

    def topological_sort(self) -> List[NodeType]:
        ### Kahn's algorithm
        # Nodes with no incoming edges
        nodes = deque(
            node
            for node in self.nodes
            if not self.incoming_edges.get(node)
        )
        # Copy edge multimaps
        outgoing_edges = {
            source: targets.copy()
            for source, targets in self.outgoing_edges.items()
        }
        incoming_edges = {
            target: sources.copy()
            for target, sources in self.incoming_edges.items()
        }
        result = []
        # While there are unprocessed nodes with no incoming edges
        while nodes:
            # Take a node with no incoming edges
            node = nodes.popleft()
            # Append it to the topo-sorted result list
            result.append(node)
            # For each outgoing edges from this node, and their targets
            node_outgoing = outgoing_edges.get(node, set())
            for downstream in node_outgoing.copy():
                # Remove outgoing edge
                downstream_incoming = incoming_edges.get(downstream, set())
                node_outgoing.remove(downstream)
                downstream_incoming.remove(node)
                # If target has no incoming edges remaining, add to the
                # "nodes with no incoming edges" list
                if not downstream_incoming:
                    nodes.append(downstream)
        # If any edges still remain in our graph, there's a cycle present
        if any(edge_set for edge_set in outgoing_edges.values()):
            raise Exception('Graph is cyclic')
        return result
