import asyncio
from collections import defaultdict
from dataclasses import field, dataclass
import inspect
import logging
from typing import Awaitable, Callable, Coroutine, Dict, Iterable, Set, Type, TypeVar

from meqtt.messages import Message

from .asyncio_task_manager import AsyncioTaskManager


_log = logging.getLogger(__name__)

MessageType = TypeVar("MessageType", bound=Message)
Handler = Callable[[MessageType], Coroutine[None, None, None]]


@dataclass
class HandlerData:
    running_instances: Set[asyncio.Task] = field(default_factory=set)
    instance_count: int = 0


class HandlerManager:
    def __init__(self):
        # Keys are the registered handlers.
        self._handlers: Dict[Handler, HandlerData] = {}
        # A map from the messages to the handlers that can handle them.
        self._message_handlers: Dict[Type[Message], Set[Handler]] = defaultdict(set)
        self._asyncio_task_manager = AsyncioTaskManager()

    @property
    def registered_handlers(self) -> Iterable[Handler]:
        return self._handlers.keys()

    @property
    def handled_message_types(self) -> Iterable[Type[Message]]:
        return (m for m, h in self._message_handlers.items() if not len(h) == 0)

    def get_running_instance_count(self, task: Handler) -> int:
        """Return the number of running instances of a task."""

        if task not in self._handlers:
            raise ValueError('Task "%s" is not registered', task.get_name())
        return len(self._handlers[task].running_instances)

    def is_handler_running(self, task: Handler) -> bool:
        """Return True if the task is currently running."""

        return self.get_running_instance_count(task) > 0

    def register_handler(self, handler: Handler):
        """Register a task with this task manager."""

        if handler in self._handlers:
            raise ValueError('Task "%s" is already registered', handler.get_name())
        self._handlers[handler] = HandlerData()
        for message_type in _get_handled_message_types(handler):
            self._message_handlers[message_type].add(handler)

    async def handle_message(self, message: Message):
        message_type = type(message)
        handlers = self._message_handlers.get(message_type, [])
        async with asyncio.TaskGroup() as tg:
            for handler in handlers:
                handler_data = self._handlers[handler]
                name = _get_handler_instance_name(handler, handler_data.instance_count)
                task = tg.create_task(handler(message), name=name)
                handler_data.running_instances.add(task)
                handler_data.instance_count += 1

    def cancel_all_handlers(self):
        """Cancel all tasks."""

        for handler_data in self._handlers.values():
            for task in handler_data.running_instances:
                task.cancel()

    async def join(self):
        """Wait for all handlers to finish."""

        await self._asyncio_task_manager.join()


def _get_handler_name(handler: Handler) -> str:
    return handler.__name__  # returns the method name


def _get_handler_instance_name(handler: Handler, current_instance_count: int) -> str:
    return f"handler-{_get_handler_name(handler)}-{current_instance_count}"


def _get_handled_message_types(handler: Handler) -> Iterable[Type[Message]]:
    method_name = handler.__name__

    # get the signature
    try:
        signature = inspect.signature(handler)
    except TypeError as exc:
        raise ValueError(f"Cannot get signature of method {method_name}") from exc

    # get the message parameter and check the number of parameters
    parameters = list(signature.parameters.values())
    if len(parameters) != 1:  # methods are bound at this point
        raise ValueError(
            f"A handler has to take a message as the only parameter "
            f"(in addition to self if it is a method): {method_name}"
        )
    message_parameter = parameters[0]

    # check if the parameter is positional
    if message_parameter.kind not in (
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.POSITIONAL_ONLY,
    ):
        raise ValueError(
            f"The message parameter of a handler has to allow being passed positionally: {method_name}"
        )

    # get and check the type annotation
    message_type = message_parameter.annotation
    has_type_annotation = message_type is not inspect.Parameter.empty
    # We only support a single message type for now even though the
    # API allows for more than one.
    # TODO: Support multiple message types (in form of a
    # typing.Union typing).
    is_message_type = issubclass(message_type, Message)
    if not has_type_annotation or not is_message_type:
        raise ValueError(
            f"The message parameter has to have a Message class as the type annotation: {method_name}"
        )

    yield message_type
