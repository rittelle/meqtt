"""The process base."""

import asyncio
import logging
import random
from typing import Iterable, Optional, Set, Type

import meqtt.connection as connection  # module import to avoid circular import
from meqtt.messages import Message

from .decorators import TYPE_ATTRIBUTE
from .handler_manager import HandlerManager
from .message_collector import MessageCollector
from .task_manager import TaskManager

_log = logging.getLogger(__name__)


class Process:
    """The base class for processes.

    Framework users should subclass this class to impelement the functionality
    of their application.

    Attributes:
        name: The name of the process.  This is used to identify the process
            on the broker.  It has to be unique to the process instance.
    """

    def __init__(self):
        # Create a unique identifier without adding a full blown UUID.
        # Kinda like a poor man's UUID.
        rand_id = random.randbytes(8).hex()
        # The name of this process.
        self.name = f"{str(type(self).__name__)}-{rand_id}"
        # The connection to the broker.
        self.__connection: Optional["connection.Connection"] = None

        # Message collections that are created dynamically, for example by
        # wait_for().
        self.__message_collectors: Set[MessageCollector] = set()
        self.__task_manager = TaskManager(
            self.name, self.__add_message_collector, self.__remove_message_collector
        )
        self.__handler_manager = HandlerManager(self.name)

        # process the decorated methods of self
        self.__scan_methods()

    @property
    def is_running(self) -> bool:
        """Checks whether the process is running."""

        return self.__connection is not None

    async def start(self, connection: "connection.Connection"):
        """Starts the process and registers ``self`` to the given connection.

        Parameters:
            connection: The connection to the broker.
        Raises:
            RuntimeError: If the process is already running.
        """

        if self.is_running:
            raise RuntimeError("Process is already running")
        self.__connection = connection
        await self.__connection.register_process(self)
        await self.on_start()

    async def stop(self):
        """Stops the process and deregisters ``self`` from the connection.

        The inverse of :py:meth:`start`.

        Raises:
            RuntimeError: If the process is not running.
        """

        if not self.is_running:
            raise RuntimeError("Process is not running")
        await self.on_stop()
        await self.__task_manager.join()  # let cancellation happen
        assert self.__connection is not None  # mostly to make mypy happy
        await self.__connection.deregister_process(self)
        self.__connection = None

    async def join(self):
        """Waits for the process to exit.

        If no tasks are registered, this method will run indefinitely.
        """

        if self.__task_manager.registered_tasks:
            # Wait for the tasks to finish.
            await self.__task_manager.join()
            # Wait for handlers that may still be running.
            await self.__handler_manager.join()
        else:
            # wait indefinitely
            _log.debug("No tasks registered, waiting indefinitely")
            await asyncio.Event().wait()

    @property
    def handled_message_classes(self) -> Iterable[Type[Message]]:
        """Return all message classes that are currently handled by this process.

        This includes all messsage types handled by handlers and collectors in
        handler and process arguments as well as dynamically added messages from
        collectors that are defined at runtime (i.e. in task or handler
        implementations).
        """

        # We use sets to avoid duplicates.
        result = set(self.__handler_manager.handled_message_types)
        for message_collection in self.__message_collectors:
            result |= message_collection.message_types
        return result

    async def handle_message(self, message: Message) -> int:
        """Handles a message.

        Calls all handlers and notifies all collectors that take messages of the
        type of ``message``.

        Parameters:
            message: The message to handle.
        Returns:
            The number of handlers that were run.
        """

        static_handlers_run = await self.__handler_manager.handle_message(message)
        dynamic_handlers_run = 0
        dynamic_handlers_total = len(self.__message_collectors)
        for message_collection in self.__message_collectors:
            if message_collection.try_push_message(message):
                dynamic_handlers_run += 1
        if dynamic_handlers_run > 0:
            _log.debug(
                "The message was handled by %d/%d dynamic handlers",
                dynamic_handlers_run,
                dynamic_handlers_total,
            )
        else:
            _log.debug(
                "None of the %d dynamic handlers handled this message",
                dynamic_handlers_total,
            )
        return static_handlers_run + dynamic_handlers_run

    async def on_start(self):
        """A callback that gets called after the process has been started.

        This callback is tasked with starting the process' tasks.  The default
        implementation just starts all registered tasks once.  User
        implementations are free to start only some of the registered tasks
        (useful if one task performs the setup for other tasks) or multiple
        instances of a given task.
        """

        for task in self.__task_manager.registered_tasks:
            if not self.__task_manager.is_task_running(task):
                self.start_task(task)

    async def on_stop(self):
        """A callback that gets called before the process is stopped.

        This callback is tasked with stopping the process' tasks.  It can be
        used to perform cleanup operations.  The default implementation does
        nothing because all tasks that are still running after this method has
        returned, are cancelled anyway.
        """

    def start_task(self, method) -> asyncio.Task:
        """Starts a new intsance of a task.

        Parameters:
            method: The task method to start.
        Returns: A asycio.Task object that can be used to wait for the task's
            completion or to cancel a specific instance.
        """

        return self.__task_manager.start_task(method)

    def stop_task(self, method_or_task):
        """Stops either all instances of a task or a specific instance.

        Parameters:
            method_or_task: Either the task method or a specific task instance
                to stop.
        """

        self.__task_manager.cancel_task(method_or_task)

    async def publish(self, message: Message):
        """Publishes a message to the broker.

        Parameters:
            message: The message to publish.
        Raises:
            RuntimeError: If the process is not running.
        """

        if self.__connection is None:
            raise RuntimeError("The process has to be started first.")
        await self.__connection.publish(message)

    # async def request(
    #     self, message_class, timeout=Union[float, datetime.timedelta]
    # ) -> Any:
    #     """Fragt eine Nachricht an und gibt die Antwort zurück.

    #     Falls timeout vorher verstrichen ist, wirft die Methode eine exception.
    #     """

    #     raise NotImplementedError()

    async def wait_for(self, *message_types: Type[Message]) -> Message:
        """Waits for a message of the given type(s) to arrive and returns it.

        If multiple message types are given, the first message of any of the
        given types is returned.

        This can be used to receive messages in tasks.

        Please note that only messages that arrive during the await get returned
        (and after a small setup time in the beginning).  So if a relevant
        message arrives when the task is not currently waiting for a message, it
        will be missed.  This method is therefore not suitable for receiving
        messages in a continuous manner.

        See collector() for a way to reliably receive all messages that arrive
        while a specific piece of code is executed.

        Parameters:
            message_types: The message types to wait for.
        Returns:
            The first message of any of the given types that arrives after the
            method has been called.
        Raises:
            RuntimeError: If the process is not running.
        """

        if self.__connection is None:
            raise RuntimeError("The process has to be started first.")
        message_collection = MessageCollector(message_types)
        await self.__add_message_collector(message_collection)
        try:
            return await message_collection.wait_for()
        finally:
            await self.__remove_message_collector(message_collection)

    async def collector(self, *message_types: Type[Message]) -> MessageCollector:
        """Returns an async context manager that can be used to receive messages.

        This can be used to receive messages in tasks.  In contrast to
        :py:meth:`wait_for`, this method can be used to receive messages in a
        continuous manner.

        The returned context manager can be used in an ``async with`` statement
        to receive messages.  The context manager returns the received messages
        in the order they arrive.  If multiple message types are given, the
        context manager returns any message of any of the given types.

        Parameters:
            message_types: The message types to wait for.
        Returns:
            An async context manager that can be used to receive messages.
        Raises:
            RuntimeError: If the process is not running.
        Example:

            async with process.collector(Message1, Message2) as collector:
                while True:
                    message = await collector.get()
                    # do something with message

        """

        if self.__connection is None:
            raise RuntimeError("The process has to be started first.")
        return MessageCollector(
            message_types,
            self.__add_message_collector,
            self.__remove_message_collector,
        )

    def __scan_methods(self):
        """Scan the methods of the class for handlers and tasks."""

        for method_name in dir(self):
            method = getattr(self, method_name)
            try:
                method_type = getattr(method, TYPE_ATTRIBUTE)
            except AttributeError:
                continue  # not a decorated method

            bound_method = method.__get__(self, self.__class__)

            match method_type:
                case "handler":
                    self.__handler_manager.register_handler(bound_method)
                case "task":
                    self.__task_manager.register_task(bound_method)

    async def __add_message_collector(self, message_collection):
        """Add a message collection the internal collection and do some setup."""

        assert self.__connection is not None
        for message_class in message_collection.message_types:
            await self.__connection.add_process_subscription(self, message_class)
        _log.debug("Installing dynamic handler")
        self.__message_collectors.add(message_collection)

    async def __remove_message_collector(self, message_collection):
        """Add a message collection the internal collection and do some cleanup."""

        assert self.__connection is not None
        _log.debug("Uninstalling dynamic handler")
        self.__message_collectors.remove(message_collection)
        for message_class in message_collection.message_types:
            await self.__connection.remove_process_subscription(self, message_class)
