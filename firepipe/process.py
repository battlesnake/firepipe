from typing import Iterable, Any, Optional, Dict
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta

from .graph import Node, Graph


TaskName = str


@dataclass
class TaskNode(Node):
    """ Node in process graph, internal use only """

    task: 'Task'

    def __hash__(self):
        return id(self)


@dataclass
class Resource():
    """ Represents some resource which limits concurrency of task execution """

    name: str


Resources = Dict[Resource, int]


@dataclass
class TaskMetrics():
    """ Metrics for a run of a task """

    # Task this represents a run of
    task_name: TaskName
    # Task succeeded or failed?
    success: Optional[bool] = None
    # Result of task / exception
    result: Optional[Any] = None
    # Time execution began
    start_time: Optional[datetime] = None
    # Time execution ended
    end_time: Optional[datetime] = None
    # Execution duration
    duration: Optional[timedelta] = None
    # Previous attempt
    previous: Optional['TaskMetrics'] = None


UpstreamTasks = Dict[TaskName, TaskMetrics]


class Task(ABC):
    """ A unit of work as part of a larger process """

    # Name of task
    name: TaskName
    # Node in graph for this task (should have backreference)
    node: TaskNode
    # Resources required by task (from resources allocated to the Process)
    resources: Resources

    def __init__(self, name: TaskName):
        self.name = name
        self.node = TaskNode(task=self)
        self.resources = dict()

    @abstractmethod
    def execute(self, context: 'ExecutionContext') -> Any: # pyright: reportUndefinedVariable=false
        pass


class Process():
    """ A set of tasks with dependencies, forming a directed acyclic graph """

    # Name of process
    name: str
    # Graph of TaskNode-s defining this process
    graph: Graph[TaskNode]
    # Resources allocated to this process
    resources: Resources

    def __init__(self, name: str, resources: Resources = None):
        self.name = name
        self.graph = Graph()
        self.resources = resources or dict()

    def add(self, task: Task) -> None:
        self.graph.add_node(task.node)

    def connect(self, upstream: Task, downstream: Task) -> None:
        self.graph.add_edge(upstream.node, downstream.node)

    def fork(self, upstream: Task, downstreams: Iterable[Task]) -> None:
        for downstream in downstreams:
            self.connect(upstream, downstream)

    def join(self, upstreams: Iterable[Task], downstream: Task) -> None:
        for upstream in upstreams:
            self.connect(upstream, downstream)
