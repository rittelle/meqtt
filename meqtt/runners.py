"""Entrypoints for the framework.

Functions in this module are used to start processes.
"""

import asyncio
import logging

from meqtt.connection import Connection, ConnectionInfo

_log = logging.getLogger(__name__)


async def launch_process(connection_info: ConnectionInfo, process) -> None:
    """Start a process in an existing event loop.

    Like :py:func:`run_process`, but asynchronous.  This allows the framework to
    be used in an existing event loop.

    Parameters:
        connection_info: Information on how to connect to the broker.
        process: The process to run.
    """

    connection = Connection(connection_info, process.name)
    try:
        async with connection:
            _log.info("Starting process %s", process.name)
            await process.start(connection)
            _log.info("Running process %s until it exits", process.name)
            await process.join()
    except Exception as exc:
        _log.exception(
            "Error while running process %s: %s (%s)", process.name, exc, str(type(exc))
        )
    else:
        _log.info("Process %s has finished normally", process.name)
    finally:
        _log.info("Stopping process %s", process.name)
        await process.stop()
        _log.info("Process %s stopped", process.name)


def run_process(connection_info: ConnectionInfo, process):
    """Start a process in new event loop.

    Creates a new event loop, and runs the process in it.  This is the main
    entrypoint for the framework if no integration with another async framework
    is required.

    Parameters:
        connection_info: Information on how to connect to the broker.
        process: The process to run.
    """

    asyncio.run(launch_process(connection_info, process))
