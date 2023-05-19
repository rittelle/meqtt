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
            _log.debug("Waitng for messages")
            assert await self.wait_for(MessageA) is message_a1
            _log.debug("Message a1 received")
            assert await self.wait_for(MessageB) is message_b
            _log.debug("Message b received")
            assert await self.wait_for(MessageA) is message_a2
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
    # Subscriptions / unsubscriptions are done for each wait_for() call
    assert connection.add_process_subscription.call_count == 3
    assert connection.remove_process_subscription.call_count == 3
