import asyncio
import logging

from meqtt.connection import Connection


_log = logging.getLogger(__name__)


async def launch_process(broker_host, process):
    _log.info("Starting process %s connecting to host %s", process.name, broker_host)
    try:
        async with Connection(broker_host, process.name) as connection:
            await process.start(connection)
            await process.join()
            await process.stop()
    except Exception as exc:
        _log.exception(
            "Error while running process %s: %s (%s)", process.name, exc, str(type(exc))
        )
        await process.kill()
        raise
    else:
        _log.info("Process %s finished", process.name)


def run_process(broker_host, process):
    asyncio.run(launch_process(broker_host, process))
