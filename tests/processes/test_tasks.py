import asyncio
from unittest.mock import MagicMock
import pytest

import meqtt
from meqtt.processes.message_collector import MessageCollector


@pytest.mark.asyncio
async def test_all_tasks_completing_default_start():
    # the default implementation of on_start() is used.
    class AProcess(meqtt.Process):
        @meqtt.task
        async def task1(self):
            await asyncio.sleep(0.05)

        @meqtt.task
        async def task2(self):
            pass

    process = AProcess()
    connection = MagicMock(spec_set=meqtt.Connection)

    async with asyncio.timeout(0.1):
        await process.start(connection)
        await process.join()
        await process.stop()


@pytest.mark.asyncio
async def test_all_tasks_completing_custom_start():
    class AProcess(meqtt.Process):
        def __init__(self, completion_order):
            super().__init__()
            self.completion_order = completion_order

        @meqtt.task
        async def task1(self):
            task2 = self.start_task(self.task2)
            await asyncio.sleep(0.01)
            # This task should complete faster if waiting for task2
            # does not work properly.
            await task2
            self.completion_order.append("task1")

        @meqtt.task
        async def task2(self):
            await asyncio.sleep(0.05)
            self.completion_order.append("task2")

        async def on_start(self):
            self.start_task(self.task1)

    completion_order = []
    process = AProcess(completion_order)
    connection = MagicMock(spec_set=meqtt.Connection)

    async with asyncio.timeout(0.1):
        await process.start(connection)
        await process.join()
        await process.stop()

    assert completion_order == ["task2", "task1"]


@pytest.mark.asyncio
async def test_message_collector_params():
    @meqtt.message("test/message/a")
    class MessageA(meqtt.Message):
        pass

    @meqtt.message("test/message/b")
    class MessageB(meqtt.Message):
        pass

    message_a = MessageA()
    message_b = MessageB()
    task_finished = False

    class AProcess(meqtt.Process):
        @meqtt.task
        async def task1(
            self,
            col_a: MessageCollector[MessageA],
            col_b: MessageCollector[MessageA, MessageB],
        ):
            await asyncio.sleep(0.05)

            # both collectors should receive message_a
            assert col_a.get_single() is message_a
            assert col_a.get_single() is message_a
            assert col_a.get_single() is None

            assert col_b.get_single() is message_a
            assert col_b.get_single() is message_b
            assert col_b.get_single() is message_a
            assert col_b.get_single() is None

            nonlocal task_finished
            task_finished = True

    async def push_messages(process):
        await asyncio.sleep(0.02)
        await process.handle_message(message_a)
        await process.handle_message(message_b)
        await process.handle_message(message_a)

    process = AProcess()
    connection = MagicMock(spec_set=meqtt.Connection)

    async with asyncio.timeout(0.1):
        await process.start(connection)
        await push_messages(process)
        await process.join()
        await process.stop()

    assert task_finished, "task1 is expected to finish"
