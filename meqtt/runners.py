import asyncio
import logging

from meqtt.connection import Connection, ConnectionInfo

_log = logging.getLogger(__name__)


async def launch_process(connection_info: ConnectionInfo, process):
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
    asyncio.run(launch_process(connection_info, process))
