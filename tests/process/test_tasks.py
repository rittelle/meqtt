import asyncio
from unittest.mock import MagicMock
import pytest

import meqtt


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
