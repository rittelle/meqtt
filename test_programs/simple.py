import asyncio
import logging

import meqtt

_log = logging.getLogger(__name__)

@meqtt.message("test/topic/rx")
class RxMessage(meqtt.Message):
    input: int

@meqtt.message("test/topic/tx")
class TxMessage(meqtt.Message):
    output: int
    text: str

@meqtt.message("test/topic/periodic")
class PeriodicMessage(meqtt.Message):
    value: int
    text: str = f"Transmit an object in the form {{ \"input\": 42 }} at {RxMessage.topic}!"

class AProcess(meqtt.Process):
    @meqtt.task
    async def task1(self):
        _log.info("Task 1 is starting")
        for i in range(10):
            await self.publish(PeriodicMessage(i))
            await asyncio.sleep(5)
        _log.info("Task 1 finished")

    @meqtt.handler
    async def on_rx(self, message: RxMessage):
        _log.info("Received rx message wit value: %d", message.input)
        await self.publish(TxMessage(message.input + 1, "Hello there!"))


def main():
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("gmqtt").setLevel(logging.INFO)
    _log.info("Starting process")
    meqtt.run_process("localhost", AProcess())

if __name__ == "__main__":
    main()
