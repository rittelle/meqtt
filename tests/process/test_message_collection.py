import asyncio
import meqtt
import pytest
from meqtt.processes.message_collection import MessageCollection


def test_sync_operation():
    @meqtt.message("test/message/a")
    class MessageA(meqtt.Message):
        pass

    @meqtt.message("test/message/b")
    class MessageB(meqtt.Message):
        pass

    message_collection = MessageCollection([MessageA, MessageB])

    with pytest.raises(LookupError):
        message_collection.pop_message()

    message_a1 = MessageA()
    message_a2 = MessageA()
    message_a3 = MessageA()
    message_b = MessageB()

    message_collection.push_message(message_a1)
    assert message_collection.pop_message() is message_a1
    with pytest.raises(LookupError):
        message_collection.pop_message()

    message_collection.push_message(message_a2)
    message_collection.push_message(message_b)
    message_collection.push_message(message_a3)
    assert message_collection.pop_message() is message_a2
    assert message_collection.pop_message() is message_b
    assert message_collection.pop_message() is message_a3
    with pytest.raises(LookupError):
        message_collection.pop_message()


@pytest.mark.asyncio
async def test_async_operation():
    @meqtt.message("test/message/a")
    class MessageA(meqtt.Message):
        pass

    message_collection = MessageCollection([MessageA])

    message_a = MessageA()

    # a message was received before the call to wait_for_message()
    message_collection.push_message(message_a)
    assert await message_collection.wait_for_message() is message_a

    # a message arrives during the await
    async def push_message():
        await asyncio.sleep(0.05)
        message_collection.push_message(message_a)

    async def wait_for_message():
        assert await message_collection.wait_for_message() is message_a

    # only one task waits for the message
    async with asyncio.TaskGroup() as tg:
        tg.create_task(push_message())
        async with asyncio.timeout(0.1):
            await wait_for_message()

    # two tasks wait for the same message
    async with asyncio.TaskGroup() as tg:
        tg.create_task(push_message())
        async with asyncio.timeout(0.1):
            await asyncio.gather(wait_for_message(), wait_for_message())


@pytest.mark.asyncio
async def test_async_cancellation():
    @meqtt.message("test/message/a")
    class MessageA(meqtt.Message):
        pass

    message_collection = MessageCollection([MessageA])

    message_a = MessageA()

    # a message arrives during the await
    async def push_message():
        await asyncio.sleep(0.10)
        message_collection.push_message(message_a)

    async def wait_for_message():
        assert await message_collection.wait_for_message() is message_a

    async def will_be_cancelled():
        with pytest.raises(asyncio.CancelledError):
            assert await message_collection.wait_for_message() is message_a

    async def cancel_task(task):
        await asyncio.sleep(0.05)
        task.cancel()

    # two tasks wait for the same message, but one task gets cancelled
    tg = asyncio.TaskGroup()
    async with tg:
        push_task = tg.create_task(push_message())
        wait_task = tg.create_task(wait_for_message())
        cancelled_task = tg.create_task(will_be_cancelled())
        cancelling_task = tg.create_task(cancel_task(cancelled_task))

        async with asyncio.timeout(0.15):
            # The cancelled error is not propagated, so gather() will finish
            # normally (in a successful test).
            await asyncio.gather(
                push_task,
                wait_task,
                cancelled_task,
                cancelling_task,
            )

    # The other tasks should be unaffected.
    assert not push_task.cancelled()
    assert not wait_task.cancelled()
