import asyncio
import inspect
import logging
import typing
from dataclasses import dataclass, field
from typing import (
    Callable,
    Coroutine,
    Dict,
    Iterable,
    Set,
    Tuple,
    Type,
)

from meqtt.messages import Message
from meqtt.utils import call_with_async_context_managers

from .asyncio_task_manager import AsyncioTaskManager
from .message_collector import MessageCollector

_log = logging.getLogger(__name__)

# Type checking of the parameters happens at registration anyway.
Task = Callable[..., Coroutine[None, None, None]]


@dataclass
class TaskData:
    # Keys are the name of the parameter and values are the type of
    # the message classes collected in a collector instance that
    # will get passed to the task when it is started.
    collector_message_types: Dict[str, Tuple[Type[Message]]]
    running_instances: Set[asyncio.Task] = field(default_factory=set)
    instance_count: int = 0


class TaskManager:
    def __init__(
        self,
        process_name: str,
        register_message_collector,
        unregister_message_collector,
    ):
        self._process_name = process_name
        self._register_message_collector = register_message_collector
        self._unregister_message_collector = unregister_message_collector
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

        message_types = _get_collector_message_types(task)
        if task in self._tasks:
            raise ValueError('Task "%s" is already registered', task.get_name())
        self._tasks[task] = TaskData(dict(message_types))

    def start_task(self, task: Task) -> asyncio.Task:
        """Start a task.

        An asyncio.Task instance is returned which can also be used to cancel task instance.
        """

        try:
            task_data = self._tasks[task]
        except KeyError:
            raise ValueError("Task is not registered")

        # Prepare the arguments for the task.
        collectors = {
            name: MessageCollector(
                message_types,
                self._register_message_collector,
                self._unregister_message_collector,
            )
            for name, message_types in task_data.collector_message_types.items()
        }

        # Create the task.
        name = _get_task_instance_name(
            task, self._process_name, task_data.instance_count
        )
        wrapped_task = call_with_async_context_managers(
            collectors.values(), task, **collectors
        )
        asyncio_task = asyncio.create_task(wrapped_task, name=name)

        # Perfom the setup
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


def _get_collector_message_types(
    task: Task,
) -> Iterable[Tuple[str, Tuple[Type[Message]]]]:
    """Return a list of the message types mapping to each collector parameter.

    Say, a task has the following signature:

        async def task(collector1: MessageCollector[Message1, Message2], collector2: MessageCollector[Message3])

    Then this function will return [(collector1, Union[Message1, Message2]), (collector2, Message3)].
    """

    task_name = _get_task_name(task)

    # get the signature
    try:
        signature = inspect.signature(task)
    except TypeError as exc:
        raise ValueError(f"Cannot get signature of task {task_name}") from exc

    # get the message parameter and check the number of parameters
    parameters = list(signature.parameters.values())

    for parameter in parameters:
        # Check if all parameters are positional. Methods are bound at this point.
        if parameter.kind not in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.POSITIONAL_ONLY,
        ):
            raise ValueError(
                f"The parameters of a task have to allow being passed positionally: {task_name}"
            )

        parameter_type = parameter.annotation

        # Check if the parameter is annotated
        has_type_annotation = parameter_type is not inspect.Parameter.empty

        # Check if the parameter is a MessageCollector
        if not has_type_annotation:
            raise ValueError("The parameters of a task have to have type annotations")

        message_types = typing.get_args(parameter_type)
        general_type = parameter_type.__origin__  # the unparameterized type
        if general_type is not MessageCollector or len(message_types) < 1:
            raise ValueError(
                "All parameters of a task have to be annotated with "
                "MessageCollector and have explicit type arguments: "
                f"(excluding self for methods): {task_name}"
            )

        yield (parameter.name, message_types)
