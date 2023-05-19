import asyncio
import pytest

from meqtt.processes.asyncio_task_manager import AsyncioTaskManager


@pytest.mark.asyncio
async def test_all_tasks_finishing():
    async def task1():
        await asyncio.sleep(0.05)

    async def task2():
        await asyncio.sleep(0.1)

    async def task3(tm):
        tm.register_task(asyncio.create_task(asyncio.sleep(0.075)))

    tm = AsyncioTaskManager()
    tm.register_task(asyncio.create_task(task1()))
    tm.register_task(asyncio.create_task(task2()))
    tm.register_task(asyncio.create_task(task3(tm)))
    await tm.join()
    # All tasks should be finished and therefore removed from the
    # task manager.
    assert len(tm.registered_tasks) == 0


@pytest.mark.asyncio
async def test_cancelled_tasks():
    non_cancelled_task_finished = False

    async def long_running_function():
        await asyncio.sleep(5)

    async def cancel_task(tm, task):
        await asyncio.sleep(0.05)
        tm.cancel_task(task)
        await asyncio.sleep(0.05)
        nonlocal non_cancelled_task_finished
        non_cancelled_task_finished = True

    long_running_task = asyncio.create_task(
        long_running_function(), name="long_running_task"
    )
    tm = AsyncioTaskManager()
    tm.register_task(long_running_task)
    tm.register_task(
        asyncio.create_task(cancel_task(tm, long_running_task), name="cancel_task")
    )
    async with asyncio.timeout(1):
        await tm.join()
    # All tasks should be finished or cancelled and therefore removed
    # from the task manager.
    assert len(tm.registered_tasks) == 0
    assert non_cancelled_task_finished


@pytest.mark.asyncio
async def test_cancelled_join():
    async def long_running_function():
        await asyncio.sleep(5)

    long_running_task = asyncio.create_task(long_running_function())

    tm = AsyncioTaskManager()
    tm.register_task(long_running_task)
    # asyncio translates the CancelledError into a TimeoutError.
    with pytest.raises(asyncio.TimeoutError):
        async with asyncio.timeout(0.1):
            await tm.join()

    assert (
        long_running_task.cancelled()
    ), "The task manager should have cancelled the task."


@pytest.mark.asyncio
async def test_cancelled_join_and_task():
    # Test the case where a task and join() are cancelled at the same
    # time.
    async def long_running_function():
        await asyncio.sleep(5)

    long_running_task = asyncio.create_task(
        long_running_function(), name="running task"
    )
    cancelled_task = asyncio.create_task(long_running_function(), name="cancelled task")

    tm = AsyncioTaskManager()
    tm.register_task(long_running_task)
    tm.register_task(cancelled_task)
    tm_task = asyncio.create_task(tm.join())

    await asyncio.sleep(0.1)

    cancelled_task.cancel()
    tm_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await tm_task

    assert cancelled_task.cancelled()
    assert (
        long_running_task.cancelled()
    ), "The task manager should have cancelled the remaining task."
    assert tm_task.cancelled()
