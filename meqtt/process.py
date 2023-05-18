import asyncio
import inspect
import itertools
import logging
from collections import defaultdict
from typing import List, Optional, Set, Type

import meqtt.connection as connection  # module import to avoid circular import
from meqtt.messages import Message

_log = logging.getLogger(__name__)


class Process:
    def __init__(self):
        # The name of this process.
        # TODO: Make sure that this is unique.
        self.name = str(type(self).__name__)
        # Dict from message types to lists of handlers that take
        # objects of those messages.  The handlers are bound to self.
        self.__handlers = defaultdict(list)
        # Dict from task methods bound to self to running a set of
        # tasks from that method.
        self.__running_tasks = {}
        # A set of tasks returned by handlers that are currently
        # running.
        self.__running_handlers: Set[asyncio.Task] = set()
        # The connection to the broker.
        self.__connection: Optional["connection.Connection"] = None

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
        await self.__cancel_remaining_tasks()
        assert self.__connection is not None  # mostly to make mypy happy
        await self.__connection.deregister_process(self)
        self.__connection = None

    async def kill(self):
        """Beendet den Prozess, die Verbindung zum Broker ist nicht mehr vorhanden."""

        await self.on_kill()
        await self.__cancel_remaining_tasks()
        self.__connection = None

    async def join(self):
        """Wartet auf das Beenden des Prozesses."""

        while True:
            running_tasks = list(
                itertools.chain.from_iterable(self.__running_tasks.values())
            )
            running_handlers = self.__running_handlers

            if running_tasks or running_handlers:
                # We may encounter tasks that are already removed from
                # self.__running_tasks or self.__running_handlers, but this
                # does not matter.
                # Especially handlers get awaited a second time.
                # We wait for handlers to finish to make sure that handlers
                # that are still running after the last task has finished
                # get to finishe aswell.
                for task in itertools.chain(running_tasks, running_handlers):
                    await self.__let_task_finish(task)
            else:
                break

    @property
    def handled_message_classes(self) -> List[Type[Message]]:
        """Gibt alle Message-Klassen zurück, die dieser Prozess verarbeiten kann."""

        return list(self.__handlers.keys())

    async def handle_message(self, message: Message) -> bool:
        """Verarbeitet eine Nachricht.

        Gibt True zurück, wenn die Nachricht erfolgreich verarbeitet
        wurde, sonst False.
        """

        message_type = type(message)
        handlers = self.__handlers.get(message_type, [])
        for handler in handlers:
            task = asyncio.create_task(handler(message))
            self.__running_handlers.add(task)
            task.add_done_callback(self.__running_handlers.discard)
            # TODO: This could be optimized by running all handlers at
            # once.  But I can't figure out how to handle
            # CancelledError in that case.  At leas not at the moment.
            await self.__let_task_finish(task)

    async def on_start(self):
        """Standard-Implementation, die alle Tasks startet."""

        for method, running_tasks in self.__running_tasks.items():
            if not running_tasks:
                await self.start_task(method)

    async def on_stop(self):
        """Standard-Implementation, die alle Tasks beendet.

        Das da stop() die verbleibenden Tasks beendet, ist es nicht
        notwendig, dies noch einmal hier zu tun.
        """

    async def on_kill(self):
        """Standard-Implementation, die alle Tasks beendet."""

    async def start_task(self, method):
        """Startet den angegeben Task."""

        task = asyncio.create_task(method())
        assert method in self.__running_tasks
        list_of_tasks = self.__running_tasks[method]
        list_of_tasks.add(task)
        task.add_done_callback(list_of_tasks.discard)

    async def stop_task(self, method):
        """Stoppt den angegeben Task."""

        await self.__cancel_task(method)

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

    @staticmethod
    def __get_message_type_for_handler(method):
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

    def __scan_methods(self):
        """Scan the methods of the class for handlers and tasks."""

        for method_name in dir(self):
            method = getattr(self, method_name)
            if hasattr(method, "_meqtt_type"):
                match method._meqtt_type:
                    case "handler":
                        message_type = self.__get_message_type_for_handler(method)
                        # bind the method to self
                        bound_method = method.__get__(self, self.__class__)
                        self.__handlers[message_type].append(bound_method)
                    case "task":
                        self.__running_tasks[method] = set()

    async def __let_task_finish(self, task: asyncio.Task) -> bool:
        """Awaits a task and handles the exceptions.

        Returns True if the task finished successfully, False otherwise.
        """

        try:
            await task
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log.exception(f"Task {task.get_name()} raised an exception: {exc}")
            return False
        else:
            return True

    async def __cancel_task(self, method):
        """Beendet alle Instanzen des angegeben Tasks."""

        for task in self.__running_tasks[method]:
            if not task.done():
                task.cancel()
                await self.__let_task_finish(task)

    async def __cancel_remaining_tasks(self):
        """Beendet alle verbleibenden Tasks."""

        for method in self.__running_tasks:
            await self.__cancel_task(method)


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
