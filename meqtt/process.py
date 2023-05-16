import asyncio
from collections import defaultdict
from typing import Optional

import itertools

from meqtt.messages import Message
from meqtt.connection import Connection


class Process:
    def __init__(self):
        # map von methode zu List[asyncio.Task]
        self.__running_tasks = defaultdict(list)
        # map von Klasse zu Handlern
        self.__handlers = {}
        # liste von gerade laufenden handlern
        self.__running_handlers = []
        self.name = str(type(self).__name__)

        self._scan_methods()

    async def start(self, connection: Connection):
        """Startet den Prozess."""

        self._connection = connection
        await self.on_start()

    async def stop(self):
        """Beendet den Prozess, verbindung zum Broker noch vorhanden."""

        await self.on_stop()
        self._kill_remaining_tasks()
        self._connection = None

    async def kill(self):
        """Beendet den Prozess, die Verbindung zum Broker ist nicht mehr vorhanden."""

        await self.on_kill()
        self._kill_remaining_tasks()
        self._connection = None

    async def join(self):
        """Wartet auf das Beenden des Prozesses."""

        tasks = list(
            itertools.chain.from_iterable(
                itertools.chain(self.__running_tasks.values(), self.__running_handlers)
            )
        )
        await asyncio.gather(*tasks)

    async def on_start(self):
        """Standard-Implementation, die alle Tasks startet."""

        for method, running_tasks in self.__running_tasks.items():
            if not running_tasks:
                await self.start_task(method)

    async def on_stop(self):
        """Standard-Implementation, die alle Tasks beendet."""

    async def on_kill(self):
        """Standard-Implementation, die alle Tasks beendet."""

    async def start_task(self, method):
        """Startet den angegeben Task."""

        task = asyncio.create_task(method())
        assert method in self.__running_tasks
        self.__running_tasks[method].append(task)

    # async def stop_task(self, task):
    #     """Stoppt den angegeben Task."""

    #     self._stop_task(task, exit=True)

    # async def _stop_task(self, task, exit: bool):
    #     """Stoppt den angegeben Task und beendet die asyncio loop, wenn exit True ist."""

    #     kill_task(self.__tasks(task))
    #     self.__tasks[method_name] = None
    #     if exit:
    #         # wenn dies der letzte verbleibende Prozess ist, schließt sich die Verbindung.
    #         self.__connection.report_shutdown()

    # async def restart_task(self, task):
    #     """Startet den angegeben Task neu."""

    #     self.stop_task(task, exit=False)
    #     self.start_task(task)

    async def publish(self, message: Message):
        """Versendet ein Nachrichtobjekt"""

        if self._connection is None:
            raise RuntimeError("The process has to be started first.")
        await self._connection.publish(message)

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

    def _scan_methods(self):
        """Scan the methods of the class for handlers and tasks."""

        for method_name in dir(self):
            method = getattr(self, method_name)
            if hasattr(method, "_meqtt_type"):
                # if method._meqtt_type == "handler":
                # TODO: get the type of the first argument of method
                # self.__handlers[message_cls] = method
                if method._meqtt_type == "task":
                    self.__running_tasks[method] = []

    def _kill_remaining_tasks(self):
        """Beendet alle verbleibenden Tasks."""

        # for method_name, task in self.__tasks.values():
        #     if task is not None and task.is_active():
        #         kill_task(task)


def task(method):
    """Dekorator, der eine Methode als Task markiert."""

    method._meqtt_type = "task"
    return method
