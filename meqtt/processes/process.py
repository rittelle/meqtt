import asyncio
import inspect
import itertools
import logging
from collections import defaultdict
from typing import Iterable, List, Optional, Set, Type

import meqtt.connection as connection  # module import to avoid circular import
from meqtt.messages import Message
from .decorators import TYPE_ATTRIBUTE
from .task_manager import TaskManager
from .handler_manager import HandlerManager

_log = logging.getLogger(__name__)


class Process:
    def __init__(self):
        # The name of this process.
        # TODO: Make sure that this is unique.
        self.name = str(type(self).__name__)
        # The connection to the broker.
        self.__connection: Optional["connection.Connection"] = None

        self.__task_manager = TaskManager()
        self.__handler_manager = HandlerManager()

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

        return self.__handler_manager.handled_message_types

    async def handle_message(self, message: Message):
        """Verarbeitet eine Nachricht."""

        await self.__handler_manager.handle_message(message)

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

    def start_task(self, method):
        """Startet den angegeben Task."""

        self.__task_manager.start_task(method)

    def stop_task(self, method):
        """Stoppt den angegeben Task."""

        self.__task_manager.cancel_task(method)

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

    # async def wait_for(
    #     self, message_class, timeout=Union[float, datetime.timedelta]
    # ) -> Any:
    #     """Wartet, bis eine Nachricht empfangen wurde und gibt sie zurück.

    #     Falls timeout vorher verstrichen ist, wirft die Methode eine exception.
    #     """

    #     raise NotImplementedError()

    # async def collect_into(self, collection, message_class):
    #     """Sammelt die angegeben Nachricht (oder eine Liste von ihnen) in eine Datenstruktur.

    #     Jede emfpangene Nachricht wird in diese Liste hinzugefügt, auch wenn der aufrufende Task
    #     gerade etwas anderes tut.
    #     """

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
