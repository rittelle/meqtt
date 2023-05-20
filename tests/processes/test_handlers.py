import logging

import meqtt
import pytest

_log = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_different_messages_in_handlers():
    @meqtt.message("test")
    class TestMessage(meqtt.Message):
        idx = 1  # allows to check for the correct message

    @meqtt.message("test2")
    class TestMessage2(meqtt.Message):
        idx = 2

    class AProcess(meqtt.Process):
        def __init__(self):
            super().__init__()
            self.calls_to_test1 = 0
            self.calls_to_test2 = 0

        @meqtt.handler
        async def on_test(self, message: TestMessage):
            assert message.idx == 1
            self.calls_to_test1 += 1

        @meqtt.handler
        async def on_test2(self, message: TestMessage2):
            assert message.idx == 2
            self.calls_to_test2 += 1

    _log.debug("Process class created")
    process = AProcess()
    _log.debug("Process instance created")
    await process.handle_message(TestMessage())
    assert process.calls_to_test1 == 1
    assert process.calls_to_test2 == 0
    await process.handle_message(TestMessage2())
    assert process.calls_to_test1 == 1
    assert process.calls_to_test2 == 1
    _log.debug("Process handlers executed successfully")


@pytest.mark.asyncio
async def test_same_message_in_handlers():
    @meqtt.message("test")
    class TestMessage(meqtt.Message):
        pass

    class AProcess(meqtt.Process):
        def __init__(self):
            super().__init__()
            self.calls_to_test1 = 0
            self.calls_to_test2 = 0

        @meqtt.handler
        async def on_test1(self, message: TestMessage):
            assert isinstance(message, TestMessage)
            self.calls_to_test1 += 1

        @meqtt.handler
        async def on_test2(self, message: TestMessage):
            assert isinstance(message, TestMessage)
            self.calls_to_test2 += 1

    _log.debug("Process class created")
    process = AProcess()
    _log.debug("Process instance created")
    await process.handle_message(TestMessage())
    assert process.calls_to_test1 == 1
    assert process.calls_to_test2 == 1
    _log.debug("Process handlers executed successfully")


@pytest.mark.asyncio
async def test_if_unrelated_handler_does_not_get_executed():
    @meqtt.message("test")
    class TestMessage(meqtt.Message):
        pass

    @meqtt.message("test2")
    class TestMessage2(meqtt.Message):
        pass

    class AProcess(meqtt.Process):
        @meqtt.handler
        async def on_test1(self, message: TestMessage):
            assert isinstance(message, TestMessage)

        @meqtt.handler
        async def on_test2(self, message: TestMessage2):
            assert isinstance(message, TestMessage2)
            assert False, "This handler should not be executed"

    _log.debug("Process class created")
    process = AProcess()
    _log.debug("Process instance created")
    await process.handle_message(TestMessage())
    _log.debug("Process handlers executed successfully")
