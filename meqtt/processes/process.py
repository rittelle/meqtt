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
        self.__task_manager = TaskManager(self.name)
        self.__message_collectors: Set[MessageCollector] = set()
        self.__handler_manager = HandlerManager(self.name)

        # process the decorated methods of self
        self.__scan_methods()

    @property
    def is_running(self) -> bool:
        """Gibt an, ob der Prozess läuft."""

        return self.__connection is not None

    async def start(self, connection: "connection.Connection"):
        """Startet den Prozess."""

        if self.is_running:
            raise RuntimeError("Process is already running")
        self.__connection = connection
        await self.__connection.register_process(self)
        await self.on_start()

    async def stop(self):
        """Beendet den Prozess, verbindung zum Broker noch vorhanden."""

        if not self.is_running:
            raise RuntimeError("Process is not running")
        await self.on_stop()
        await self.__task_manager.join()  # let cancellation happen
        assert self.__connection is not None  # mostly to make mypy happy
        await self.__connection.deregister_process(self)
        self.__connection = None

    async def join(self):
        """Wartet auf das Beenden des Prozesses."""

        # Wait for the tasks to finish.
        await self.__task_manager.join()
        # Wait for handlers that may still be running.
        await self.__handler_manager.join()

    @property
    def handled_message_classes(self) -> Iterable[Type[Message]]:
        """Gibt alle Message-Klassen zurück, die dieser Prozess verarbeiten kann."""

        # We use sets to avoid duplicates.
        result = set(self.__handler_manager.handled_message_types)
        for message_collection in self.__message_collectors:
            result |= message_collection.message_types
        return result

    async def handle_message(self, message: Message) -> bool:
        """Verarbeitet eine Nachricht."""

        message_handled = False
        await self.__handler_manager.handle_message(message)
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
            message_handled = True
        else:
            _log.debug(
                "None of the %d dynamic handlers handled this message",
                dynamic_handlers_total,
            )
        return message_handled

    async def on_start(self):
        """Standard-Implementation, die alle Tasks startet, welche noch nicht gestarted wurden."""

        for task in self.__task_manager.registered_tasks:
            if not self.__task_manager.is_task_running(task):
                self.start_task(task)

    async def on_stop(self):
        """Standard-Implementation, die alle Tasks beendet.

        Das da stop() die verbleibenden Tasks beendet, ist es nicht
        notwendig, dies noch einmal hier zu tun.
        """

    def start_task(self, method) -> asyncio.Task:
        """Startet den angegeben Task.

        An asycio.Task object is returned that can be used to wait for the
        task's completion or to cancel a specific instance.
        """

        return self.__task_manager.start_task(method)

    def stop_task(self, method_or_task):
        """Stoppt den angegeben Task."""

        self.__task_manager.cancel_task(method_or_task)

    async def publish(self, message: Message):
        """Versendet ein Nachrichtobjekt"""

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
        """Returns an async context manager that can be used to receive messages."""

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
