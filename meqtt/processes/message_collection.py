import asyncio
from typing import Iterable, List, Set, Type

from meqtt.messages import Message


class MessageCollection:
    """A collection of Messages of specific types.

    The collection acts as a queue. Messages are retrieved in the order they
    were added to the collection.

    It also provides a way to await a message of the specified types.
    """

    def __init__(self, message_types: Iterable[Type[Message]]):
        # types to collect
        self.message_types = set(message_types)
        # collected but not retrieved messages
        self.messages: List[Message] = []
        # A list of futures that are used to wait for a message.  It is only
        # populated when a message is awaited.
        self._futures: Set[asyncio.Future[Message]] = set()

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

    def pop_message(self) -> Message:
        """Return the next message in the collection.

        Raises LookupError if the collection is empty.
        """

        try:
            return self.messages.pop(0)
        except IndexError:
            raise LookupError("No messages left")

    async def wait_for_message(self) -> Message:
        """Wait for a new message and return it."""

        # If there is a message waiting, return it immediately.
        try:
            return self.pop_message()
        except LookupError:
            pass

        # Otherwise, create a new future and wait for it.
        future = asyncio.get_event_loop().create_future()
        self._futures.add(future)
        return await future
