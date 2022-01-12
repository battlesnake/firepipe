from typing import List
import subprocess

import logging

from firepipe.orchestrator import Orchestrator, Process, Task, TaskStatus, ExecutionContext
from firepipe import visualiser


class ExecTask(Task):
    """ Executes a subprocess """

    def __init__(self, name: str, args: List[str]):
        super().__init__(name)
        self.args = args

    def execute(self, context: ExecutionContext) -> None: # pylint: disable=unused-argument
        retcode = subprocess.call(self.args)
        if retcode != 0:
            raise Exception(f'Subprocess exited with code {retcode}', retcode)


def main():

    pylint = ExecTask('Lint', ['pylint', './'])
    pyright = ExecTask('Type-check', ['pyright'])
    pytest = ExecTask('Test', ['pytest', '-v', '-s', '--log-cli-level=info', '.'])
    process = Process('Continuous Integration')
    process.add(pylint)
    process.add(pyright)
    process.add(pytest)
    orchestrator = Orchestrator(process)
    fail = False
    if not orchestrator.run():
        fail = True
        logging.error('Orchestrator failed')
    failed = []
    for node in orchestrator.process.graph.nodes:
        task = node.task
        metrics = orchestrator.get_task_metrics(task)
        if metrics.success is not True:
            fail = True
            logging.error('Task %s failed: %s', task.name, metrics.result)
            failed.append(task.name)

    list(map(print, visualiser.render_process(orchestrator.process)))
    list(map(print, visualiser.render_orchestrator(orchestrator)))

    if fail:
        raise Exception('Process failed due to errors in tasks: ' + ', '.join(failed))


if __name__ == '__main__':
    main()
