import asyncio
import inspect
import itertools
from collections import defaultdict
from typing import List, Optional, Type

import meqtt.connection as connection  # module import to avoid circular import
from meqtt.messages import Message


class Process:
    def __init__(self):
        # Dict from task methods bound to self to running tasks of
        # that method.
        self.__running_tasks = {}
        # Dict from message types to lists of handlers that take
        # objects of those messages.  The handlers are bound to self.
        self.__handlers = defaultdict(list)
        # A list of tasks returned by handlers that are currently
        # running.
        self.__running_handlers: List[asyncio.Task] = []
        # The name of this process.
        # TODO: Make sure that this is unique.
        self.name = str(type(self).__name__)

        # The connection to the broker.
        self._connection: Optional["connection.Connection"] = None

        self._scan_methods()

    @property
    def is_running(self) -> bool:
        """Gibt an, ob der Prozess läuft."""

        return self._connection is not None

    async def start(self, connection: "connection.Connection"):
        """Startet den Prozess."""

        if self.is_running:
            raise RuntimeError("Process is already running")
        self._connection = connection
        await self.on_start()

    async def stop(self):
        """Beendet den Prozess, verbindung zum Broker noch vorhanden."""

        if not self.is_running:
            raise RuntimeError("Process is not running")
        await self.on_stop()
        self._kill_remaining_tasks()
        self._connection = None

    @property
    def message_classes(self) -> List[Type[Message]]:
        """Gibt alle Message-Klassen zurück, die dieser Prozess verarbeiten kann."""

        return list(self.__handlers.keys())

    async def kill(self):
        """Beendet den Prozess, die Verbindung zum Broker ist nicht mehr vorhanden."""

        await self.on_kill()
        self._kill_remaining_tasks()
        self._connection = None

    async def handle_message(self, message: Message):
        """Verarbeitet eine Nachricht."""

        message_type = type(message)
        handlers = self.__handlers.get(message_type, [])
        for handler in handlers:
            await handler(message)

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

    @staticmethod
    def _get_message_type_for_handler(method):
        method_name = method.__name__

        # get the signature
        try:
            signature = inspect.signature(method)
        except TypeError as exc:
            raise ValueError(f"Cannot get signature of method {method_name}") from exc

        # get the message parameter and check the number of parameters
        parameters = list(signature.parameters.values())
        if len(parameters) != 1:  # methods are bound at this point
            raise ValueError(
                f"A handler has to take a message as the only parameter in addition to self: {method_name}"
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
        is_message_type = issubclass(message_type, Message)
        if not has_type_annotation or not is_message_type:
            raise ValueError(
                f"The message parameter has to have a Message class as the type annotation: {method_name}"
            )

        return message_type

    def _scan_methods(self):
        """Scan the methods of the class for handlers and tasks."""

        for method_name in dir(self):
            method = getattr(self, method_name)
            if hasattr(method, "_meqtt_type"):
                match method._meqtt_type:
                    case "handler":
                        message_type = self._get_message_type_for_handler(method)
                        # bind the method to self
                        bound_method = method.__get__(self, self.__class__)
                        self.__handlers[message_type].append(bound_method)
                    case "task":
                        self.__running_tasks[method] = []

    def _kill_remaining_tasks(self):
        """Beendet alle verbleibenden Tasks."""

        # for method_name, task in self.__tasks.values():
        #     if task is not None and task.is_active():
        #         kill_task(task)


def task(method):
    """Dekorator, der eine Methode als Task markiert.

    Tasks müssen asynchron sein.
    """

    if not inspect.iscoroutinefunction(method):
        raise ValueError(f"Tasks have to be async: {method.__name__}")
    method._meqtt_type = "task"
    return method


def handler(method):
    """Dekoator, der eine Methode als Handler markiert."""

    if not inspect.iscoroutinefunction(method):
        raise ValueError(f"Tasks have to be async: {method.__name__}")
    method._meqtt_type = "handler"
    return method
