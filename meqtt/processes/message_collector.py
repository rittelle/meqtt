import asyncio
from typing import (
    AsyncContextManager,
    Awaitable,
    Callable,
    Generic,
    Iterable,
    List,
    Optional,
    Self,
    Set,
    Type,
    TypeVarTuple,
)

from meqtt.messages import Message

MessageCls = TypeVarTuple("MessageCls")


class MessageCollector(Generic[*MessageCls], AsyncContextManager):
    """A collection of Messages of specific types.

    The collection acts as a queue. Messages are retrieved in the order they
    were added to the collection.

    It also provides a way to await a message of the specified types.

    The class can also be used as an async context manager if the callbacks are
    provided.
    """

    def __init__(
        self,
        # TODO: Make sure that this is the same as the type arguments (if specified)
        message_types: Iterable[Type[Message]],
        enter_callback: Optional[Callable[[Self], Awaitable[None]]] = None,
        exit_callback: Optional[Callable[[Self], Awaitable[None]]] = None,
    ):
        # types to collect
        self.message_types = set(message_types)
        # callback to call when entering the context
        self._enter_callback = enter_callback
        # callback to call when exiting the context
        self._exit_callback = exit_callback
        # collected but not retrieved messages
        self.messages: List[Message] = []
        # A list of futures that are used to wait for a message.  It is only
        # populated when a message is awaited.
        self._futures: Set[asyncio.Future[Message]] = set()

    async def __aenter__(self):
        if self._enter_callback:
            await self._enter_callback(self)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._exit_callback:
            await self._exit_callback(self)

    def try_push_message(self, message: Message):
        """Add a message to the collection if it is of a type that is collected.

        Returns True if the message was collected, False otherwise.
        """

        if type(message) not in self.message_types:
            return False

        # If there is at least one future waiting for a message, complete it.
        if self._futures:
            for future in self._futures:
                if not future.done():
                    future.set_result(message)
            self._futures.clear()
        else:
            self.messages.append(message)
        return True

    def get_single(self) -> Message:
        """Return the next message in the collection.

        Raises LookupError if the collection is empty.
        """

        try:
            return self.messages.pop(0)
        except IndexError:
            raise LookupError("No messages left")

    def get_all(self) -> Iterable[Message]:
        """Return all messages in the collection."""

        messages = self.messages
        self.messages = []
        return messages

    async def wait_for(self) -> Message:
        """Wait for a new message and return it."""

        # If there is a message waiting, return it immediately.
        try:
            return self.get_single()
        except LookupError:
            pass

        # Otherwise, create a new future and wait for it.
        future = asyncio.get_event_loop().create_future()
        self._futures.add(future)
        return await future
