from typing import List, Generator
import logging

from tabulate import tabulate

from .process import Process
from .orchestrator import Orchestrator


def center_text(name: str, width: int) -> str:
    if len(name) > width - 2:
        name = name[width - 5] + '...'
    space = width - len(name)
    left = space // 2
    right = space - left
    name = (' ' * left) + name + (' ' * right)
    return name


def render_process(process: Process, node_width: int = 20, row_width: int = 78) -> Generator[str, None, None]:
    committed = set()
    layers = []
    layer = []
    nodes = process.graph.topological_sort()
    for node in nodes:
        if all(other_node in committed for other_node in process.graph.incoming_edges[node]):
            layer.append(node)
        else:
            for previous_node in layer:
                committed.add(previous_node)
            layers.append(sorted(layer, key=lambda n: n.task.name))
            layer = [node]
    if layer:
        layers.append(layer)
    logging.debug('Generating simplified graph, with possibly innacurate dependencies (edges)')
    vert_bar = center_text('|', node_width)
    prev_nodes = 0
    for idx, layer in enumerate(layers):
        names = ''.join([center_text(node.task.name, node_width) for node in layer])
        bars = vert_bar * len(layer)
        if idx > 0 and prev_nodes != len(layer):
            split = ('-' * node_width) * (max(len(layer), prev_nodes) - 1)
            yield center_text(split, row_width)
            yield center_text(bars, row_width)
        yield center_text(names, row_width)
        if idx < len(layers) - 1:
            yield center_text(bars, row_width)
        prev_nodes = len(layer)
    yield ''


def render_orchestrator(orchestrator: Orchestrator) -> Generator[str, None, None]:
    table: List[List[str]] = [['Name', 'State', 'Start', 'Finish', 'Duration', 'Blockage reason']]
    for info in orchestrator.task_state_list:
        table += [[str(x) for x in (
            info.task.name,
            info.status,
            info.metrics.start_time,
            info.metrics.end_time,
            info.metrics.duration,
            orchestrator.get_blockage_reason(info) or ''
        )]]
    yield 'Task summary'
    yield ''
    for line in tabulate(table).split('\n'):
        yield line
    yield ''
