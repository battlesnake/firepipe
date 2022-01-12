import logging
import multiprocessing
from queue import Empty

from ..firepipe import visualiser

from ..firepipe.orchestrator import Orchestrator, Process, Task, TaskStatus, ExecutionContext


class EchoTask(Task):
    """ Dummy task that just returns its argument """

    def __init__(self, name: str, value: str):
        super().__init__(name)
        self.value = value

    def execute(self, context: ExecutionContext) -> str:
        return self.value


class QueuePushTask(Task):
    """ Task that pushes value to a queue """

    def __init__(self, name: str, queue: multiprocessing.Queue):
        super().__init__(name)
        self.queue = queue

    def execute(self, context: ExecutionContext) -> str:
        self.queue.put(self.name, block=False)
        return self.name


def post_test(orchestrator):
    def write(line):
        logging.debug('Visualisation: %s', line)
    list(map(write, visualiser.render_process(orchestrator.process)))
    list(map(write, visualiser.render_orchestrator(orchestrator)))



def test_echo_task():
    task = EchoTask('echo', 'hello')
    process = Process('echo test')
    process.add(task)
    orchestrator = Orchestrator(process)
    assert orchestrator.run()
    state = orchestrator.get_task_status(task)
    metrics = orchestrator.get_task_metrics(task)
    assert state == TaskStatus.COMPLETED
    assert metrics.success
    assert metrics.result == 'hello'

    post_test(orchestrator)


def test_serial_tasks():
    queue = multiprocessing.Queue()
    task1 = QueuePushTask('1', queue)
    task2 = QueuePushTask('2', queue)
    task3 = QueuePushTask('3', queue)
    process = Process('serial test')
    process.connect(task1, task2)
    process.connect(task2, task3)
    orchestrator = Orchestrator(process)
    result = []
    assert orchestrator.run()
    try:
        while True:
            result.append(queue.get(block=False))
    except Empty:
        pass
    for task in (task1, task2, task3):
        assert orchestrator.get_task_status(task) == TaskStatus.COMPLETED
        assert orchestrator.get_task_metrics(task).success
    assert result == ['1', '2', '3']

    post_test(orchestrator)


def test_parallel_tasks():
    queue = multiprocessing.Queue()
    task1 = QueuePushTask('1', queue)
    task2a = QueuePushTask('2a', queue)
    task2b = QueuePushTask('2b', queue)
    task3 = QueuePushTask('3', queue)
    task4a = QueuePushTask('4a', queue)
    task4b = QueuePushTask('4b', queue)
    task4c = QueuePushTask('4c', queue)
    task5 = QueuePushTask('5', queue)
    process = Process('serial test')
    process.fork(task1, [task2a, task2b])
    process.join([task2a, task2b], task3)
    process.fork(task3, [task4a, task4b, task4c])
    process.join([task4a, task4b, task4c], task5)
    orchestrator = Orchestrator(process)
    result = []
    assert orchestrator.run()
    try:
        while True:
            result.append(queue.get(block=False))
    except Empty:
        pass
    for node in orchestrator.process.graph.nodes:
        assert orchestrator.get_task_status(node.task) == TaskStatus.COMPLETED
        assert orchestrator.get_task_metrics(node.task).success
    assert ''.join([name[0] for name in result]) == '12234445'

    post_test(orchestrator)


def test_complex_tasks():
    queue = multiprocessing.Queue()
    task1 = QueuePushTask('1', queue)
    task2a = QueuePushTask('2a', queue)
    task2b = QueuePushTask('2b', queue)
    task2c = QueuePushTask('2c', queue)
    task3 = QueuePushTask('3', queue)
    task4a = QueuePushTask('4a', queue)
    task4b = QueuePushTask('4b', queue)
    task4c = QueuePushTask('4c', queue)
    task5a = QueuePushTask('4d', queue)
    task5b = QueuePushTask('4e', queue)
    task5c = QueuePushTask('4f', queue)
    task6 = QueuePushTask('5', queue)
    process = Process('serial test')
    process.fork(task1, [task2a, task2b])
    process.connect(task2b, task2c)
    process.join([task2a, task2c], task3)
    process.fork(task3, [task4a, task4b, task4c])
    process.connect(task4a, task5a)
    process.connect(task4b, task5b)
    process.connect(task4c, task5c)
    process.join([task5a, task5b, task5c], task6)
    orchestrator = Orchestrator(process)
    result = []
    assert orchestrator.run()
    try:
        while True:
            result.append(queue.get(block=False))
    except Empty:
        pass
    for node in orchestrator.process.graph.nodes:
        assert orchestrator.get_task_status(node.task) == TaskStatus.COMPLETED
        assert orchestrator.get_task_metrics(node.task).success
    assert ''.join([name[0] for name in result]) == '122234444445'

    post_test(orchestrator)
