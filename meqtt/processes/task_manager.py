import asyncio
import logging
from dataclasses import dataclass, field
from typing import Callable, Coroutine, Dict, Iterable, Set

from .asyncio_task_manager import AsyncioTaskManager

_log = logging.getLogger(__name__)

Task = Callable[[], Coroutine[None, None, None]]


@dataclass
class TaskData:
    running_instances: Set[asyncio.Task] = field(default_factory=set)
    instance_count: int = 0


class TaskManager:
    def __init__(self, process_name: str):
        self._process_name = process_name
        # Keys are the registered tasks.
        self._tasks: Dict[Task, TaskData] = {}
        self._asyncio_task_manager = AsyncioTaskManager()

    @property
    def registered_tasks(self) -> Iterable[Task]:
        return self._tasks.keys()

    def get_running_instance_count(self, task: Task) -> int:
        """Return the number of running instances of a task."""

        if task not in self._tasks:
            raise ValueError('Task "%s" is not registered', task.get_name())
        return len(self._tasks[task].running_instances)

    def is_task_running(self, task: Task) -> bool:
        """Return True if the task is currently running."""

        return self.get_running_instance_count(task) > 0

    def register_task(self, task: Task):
        """Register a task with this task manager."""

        if task in self._tasks:
            raise ValueError('Task "%s" is already registered', task.get_name())
        self._tasks[task] = TaskData()

    def start_task(self, task: Task) -> asyncio.Task:
        """Start a task.

        An asyncio.Task instance is returned which can also be used to cancel task instance.
        """

        try:
            task_data = self._tasks[task]
        except KeyError:
            raise ValueError("Task is not registered")
        name = _get_task_instance_name(
            task, self._process_name, task_data.instance_count
        )
        asyncio_task = asyncio.create_task(task(), name=name)
        _log.debug('Started task "%s" as "%s"', _get_task_name(task), name)
        self._asyncio_task_manager.register_task(asyncio_task)
        task_data.running_instances.add(asyncio_task)
        task_data.instance_count += 1
        return asyncio_task

    def cancel_task(self, task: Task | asyncio.Task):
        """Cancel all instances of a task."""

        if isinstance(task, Task):
            if task not in self._tasks:
                raise ValueError('Task "%s" is not registered', task.get_name())
            running_instances = list(self._tasks[task].running_instances)
            _log.debug(
                'Cancelling %d instances of task "%s"',
                len(running_instances),
                _get_task_name(task),
            )
            for asyncio_task in self._tasks[task].running_instances:
                self._asyncio_task_manager.cancel_task(asyncio_task)
        elif isinstance(task, asyncio.Task):
            all_asyncio_tasks = self._asyncio_task_manager.registered_tasks
            if task not in all_asyncio_tasks:
                raise ValueError(
                    'Task instance "%s" does not appear to belong to a registered task',
                    task.get_name(),
                )
            self._asyncio_task_manager.cancel_task(task)
        else:
            raise ValueError("The argument is not a valid task or task instance")

    def cancel_all_tasks(self):
        """Cancel all tasks."""

        _log.debug("Cancelling all running tasks")
        for task in self._tasks:
            self.cancel_task(task)

    async def join(self):
        """Wait for all tasks to finish."""

        _log.debug("Waiting for all tasks to finish")
        await self._asyncio_task_manager.join()


def _get_task_name(task: Task) -> str:
    return task.__name__  # returns the method name


def _get_task_instance_name(
    task: Task, process_name: str, current_instance_count: int
) -> str:
    return f"{process_name}-task-{_get_task_name(task)}-{current_instance_count}"
