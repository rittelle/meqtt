import asyncio
import logging

from meqtt.connection import Connection, ConnectionInfo

_log = logging.getLogger(__name__)


async def launch_process(connection_info: ConnectionInfo, process):
    try:
        async with Connection(connection_info, process.name) as connection:
            _log.info("Starting process %s", process.name)
            await process.start(connection)
            _log.info("Running processes until they exit")
            await process.join()
            _log.info("Stopping process %s", process.name)
            await process.stop()
            _log.info("All processes have exited normally")
    except Exception as exc:
        _log.exception(
            "Error while running process %s: %s (%s)", process.name, exc, str(type(exc))
        )
        await process.kill()
        _log.info("Process %s killed", process.name)
        raise
    else:
        _log.info("Process %s finished", process.name)


def run_process(connection_info: ConnectionInfo, process):
    asyncio.run(launch_process(connection_info, process))
