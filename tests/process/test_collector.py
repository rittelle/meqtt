import logging
import meqtt
import pytest
import asyncio
from unittest.mock import MagicMock

_log = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_wait_for():
    @meqtt.message("test/message/a")
    class MessageA(meqtt.Message):
        pass

    @meqtt.message("test/message/b")
    class MessageB(meqtt.Message):
        pass

    # an unrelated class
    @meqtt.message("test/message/c")
    class MessageC(meqtt.Message):
        pass

    message_a1 = MessageA()
    message_a2 = MessageA()
    message_b = MessageB()
    message_c = MessageC()

    class AProcess(meqtt.Process):
        @meqtt.task
        async def task1(self):
            async with await self.collector(MessageA, MessageB) as collector:
                _log.debug("Waitng for messages")
                assert await collector.wait_for(MessageA) is message_a1
                _log.debug("Message a1 received")
                assert await collector.wait_for(MessageB) is message_b
                _log.debug("Message b received")
                assert await collector.wait_for(MessageA) is message_a2
                _log.debug("Message a2 received, done")

    process = AProcess()
    connection = MagicMock(spec_set=meqtt.Connection)

    async def push_message():
        await asyncio.sleep(0.01)  # make sure that task1 is ready.
        _log.debug("Pushing message a1")
        await process.handle_message(message_a1)
        await asyncio.sleep(0.01)  # let task1 handle the message
        _log.debug("Pushing message b")
        await process.handle_message(message_b)
        await asyncio.sleep(0.01)  # let task1 handle the message
        _log.debug("Pushing message c")
        await process.handle_message(message_c)  # should be ignored
        await asyncio.sleep(0.01)  # let task1 handle the message
        _log.debug("Pushing message a2")
        await process.handle_message(message_a2)
        await asyncio.sleep(0.01)  # let task1 handle the message

    async def run_process():
        _log.debug("Starting process")
        await process.start(connection)
        _log.debug("Running process")
        await process.join()
        _log.debug("Stopping process")
        await process.stop()

    async with asyncio.timeout(0.1):
        await asyncio.gather(push_message(), run_process())

    # check the calls to the mock methods
    # There should only be one subscription/unsubscription for each message
    # type.
    assert connection.add_process_subscription.call_count == 2
    assert connection.remove_process_subscription.call_count == 2


@pytest.mark.asyncio
async def test_exception_in_with_body():
    @meqtt.message("test/message/a")
    class MessageA(meqtt.Message):
        pass

    class AProcess(meqtt.Process):
        @meqtt.task
        async def task1(self):
            class MyException(Exception):
                pass

            with pytest.raises(MyException):
                async with await self.collector(MessageA) as collector:
                    raise MyException()

    process = AProcess()
    connection = MagicMock(spec_set=meqtt.Connection)

    async def run_process():
        _log.debug("Starting process")
        await process.start(connection)
        _log.debug("Running process")
        await process.join()
        _log.debug("Stopping process")
        await process.stop()

    async with asyncio.timeout(0.1):
        await asyncio.gather(run_process())

    # check if the unsubscribe was called
    assert connection.add_process_subscription.call_count == 1
    assert connection.remove_process_subscription.call_count == 1


@pytest.mark.asyncio
async def test_get_without_filters():
    @meqtt.message("test/message/a")
    class MessageA(meqtt.Message):
        pass

    @meqtt.message("test/message/b")
    class MessageB(meqtt.Message):
        pass

    # an unrelated class
    @meqtt.message("test/message/c")
    class MessageC(meqtt.Message):
        pass

    message_a1 = MessageA()
    message_a2 = MessageA()
    message_b = MessageB()
    message_c = MessageC()

    class AProcess(meqtt.Process):
        @meqtt.task
        async def task1(self):
            async with await self.collector(MessageA, MessageB) as collector:
                # no messages should be received, yet
                assert collector.get_single() is None
                assert list(collector.get_all()) == []

                # wait for the first messages
                assert asyncio.sleep(0.10)
                assert collector.get_single() is message_a1
                assert list(collector.get_all()) == [message_b, message_a2]

                # no more messages should remain after get_all()
                assert collector.get_single() is None
                assert list(collector.get_all()) == []

    process = AProcess()
    connection = MagicMock(spec_set=meqtt.Connection)

    async def push_message():
        await asyncio.sleep(0.05)
        _log.debug("Pushing messages")
        await process.handle_message(message_a1)
        await process.handle_message(message_b)
        await process.handle_message(message_c)  # should be ignored
        await process.handle_message(message_a2)

    async def run_process():
        _log.debug("Starting process")
        await process.start(connection)
        _log.debug("Running process")
        await process.join()
        _log.debug("Stopping process")
        await process.stop()

    async with asyncio.timeout(0.1):
        await asyncio.gather(push_message(), run_process())

    # check the calls to the mock methods
    # There should only be one subscription/unsubscription for each message
    # type.
    assert connection.add_process_subscription.call_count == 2
    assert connection.remove_process_subscription.call_count == 2
