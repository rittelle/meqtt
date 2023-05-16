import asyncio
import logging

import meqtt

_log = logging.getLogger(__name__)

@meqtt.message("/test/topic")
class AMessage(meqtt.Message):
    value: int

class AProcess(meqtt.Process):
    @meqtt.task
    async def task1(self):
        _log.info("Task 1 is starting")
        for i in range(10):
            await self.publish(AMessage(i))
            await asyncio.sleep(5)
        _log.info("Task 1 finished")


def main():
    logging.basicConfig(level=logging.DEBUG)
    _log.info("Starting process")
    meqtt.run_process("localhost", AProcess())

if __name__ == "__main__":
    main()
