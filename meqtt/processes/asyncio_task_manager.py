import asyncio
import logging
from typing import Awaitable, Optional, Set, Tuple, TypeVar

_log = logging.getLogger(__name__)


class AsyncioTaskManager:
    """Similar to asyncio.TaskGroup but more tailored to our needs."""

    def __init__(self):
        # The set of tasks that are currently running.
        self.registered_tasks: Set[asyncio.Task] = set()
        # We track the of cancelled tasks so that we can
        # distinguish between tasks that were cancelled by
        # AsyncioTaskManager.cancel_task() and tasks that were
        # cancelled because join() was cancelled.
        self._num_tasks_cancelled = 0

    def register_task(self, task: asyncio.Task):
        """Register a task with this task manager."""

        if task in self.registered_tasks:
            raise ValueError('Task "%s" is already registered', task.get_name())
        self.registered_tasks.add(task)
        task.add_done_callback(self._log_task_result)
        _log.debug('Registered task "%s"', task.get_name())

    def cancel_task(self, task: asyncio.Task):
        """Cancel a tasks and await it to immediatly let it return."""

        if task not in self.registered_tasks:
            raise LookupError('Task "%s" is not registered', task.get_name())
        _log.debug('Cancelling task "%s"', task.get_name())
        task.cancel()
        self._num_tasks_cancelled += 1
        # we let join() collect the cancelled task

    async def join(self):
        """Wait for all tasks to finish."""

        # the outer loop is necessary as the set of running tasks may
        # change during the iteration.
        while self.registered_tasks:
            # We wrap the task so that Exceptions from the task are returned
            # instead of being raised. This allows us to distinguish between
            # tasks that were cancelled by AsyncioTaskManager.cancel_task()
            # and tasks that were cancelled because join() was cancelled.
            tasks = self.registered_tasks.copy()
            wrapped_tasks = [
                asyncio.shield(
                    _collect_exceptions_and_return_with_const(t, t.get_name())
                )
                for t in tasks
            ]
            _log.debug("Waiting for %d tasks to finish", len(wrapped_tasks))
            try:
                for awaitable in asyncio.as_completed(wrapped_tasks):
                    exception, task_name = await awaitable
                    if exception is None:
                        pass
                    elif isinstance(exception, asyncio.CancelledError):
                        _log.debug('Task "%s" was cancelled', task_name)
                    else:
                        _log.exception(
                            'Task "%s" failed: %s (%s)',
                            task_name,
                            exception,
                            str(type(exception)),
                            exc_info=exception,
                        )
            except asyncio.CancelledError:
                _log.debug("join() was cancelled, cancelling all managed tasks")
                for task in self.registered_tasks:
                    task.cancel()
                    try:
                        _log.debug(
                            "Waiting for task %s to being cancelled", task.get_name()
                        )
                        await task
                    except asyncio.CancelledError:
                        pass
                    except Exception as exception:
                        _log.exception(
                            'Task "%s" failed during cancellation: %s (%s)',
                            task.get_name(),
                            exception,
                            str(type(exception)),
                            exc_info=exception,
                        )
                _log.debug("All managed tasks cancelled, propagating the cancellation")
                raise
            else:
                self.registered_tasks -= tasks

    def _log_task_result(self, task: asyncio.Task):
        """Logs a message when a task finishes."""

        if task.cancelled:
            _log.debug("Task %s cancelled", task.get_name())
        elif (exception := task.exception()) is not None:
            _log.exception(
                "Task %s failed: %s (%s)",
                task.get_name(),
                task.exception(),
                str(type(task.exception())),
                exc_info=exception,
            )
        else:
            _log.debug("Task %s finished successfully", task.get_name())


async def run_task_and_log_exceptions(task: asyncio.Task):
    """Run a task and logs any exceptions that occur."""

    try:
        await task
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _log.exception(
            "Error while running task %s: %s (%s)",
            task.get_name(),
            exc,
            str(type(exc)),
            exc_info=exc,
        )
        raise
    else:
        _log.info("Task %s finished", task.get_name())


_T = TypeVar("_T")


async def _collect_exceptions_and_return_with_const(
    awaitable: Awaitable[None], const: _T
) -> Tuple[Optional[BaseException], _T]:
    """Collects exceptions from an awaitable and return them.

    The second argument is returned together with the exception.
    """

    try:
        await awaitable
    except BaseException as exc:
        return exc, const
    else:
        return None, const
