import logging
from typing import AsyncContextManager, Dict, List, Type
import gmqtt

from meqtt.messages import Message, from_json, to_json
from meqtt.process import Process
from meqtt.utils import get_type_name

_log = logging.getLogger(__name__)


class Connection(AsyncContextManager):
    def __init__(self, host, client_id):
        self._client = gmqtt.Client(client_id)
        self._host = host

        # processes using this connection
        self._processes: List[Process] = []

        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_disconnect = self._on_disconnect
        self._client.on_subscribe = self._on_subscribe

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.disconnect()

    async def connect(self):
        _log.info("Connecting to MQTT broker on %s", self._host)
        await self._client.connect(host=self._host)

    async def disconnect(self):
        _log.info("Disconnecting from MQTT broker")
        await self._client.disconnect()

    async def subscribe_to(self, message_cls: Type[Message]):
        topic = message_cls.topic
        _log.debug(
            'Message type %s matches to topic "%s"',
            get_type_name(message_cls),
            topic,
        )
        _log.info("Subscribing to topic %s", topic)
        self._client.subscribe(topic, qos=2)

    async def unsubscribe_from(self, message_cls: Type[Message]):
        topic = message_cls.topic
        _log.debug(
            'Message type %s matches to topic "%s"',
            get_type_name(message_cls),
            topic,
        )
        _log.info("Unsubscribing from topic %s", topic)
        self._client.unsubscribe(topic)

    async def publish(self, message: Message):
        topic, payload = message.topic, to_json(message)
        _log.debug('Publishing message on topic "%s" with payload %s', topic, payload)
        self._client.publish(topic, payload, qos=2)  # exactly once

    async def register_process(self, process: Process):
        message_classes = process.message_classes
        _log.info(
            "Registering process %s which handles %d message types",
            process.name,
            len(message_classes),
        )
        for message_cls in message_classes:
            if not any(message_cls in p.message_classes for p in self._processes):
                await self.subscribe_to(message_cls)
            else:
                _log.debug(
                    "Message type %s already is subscribed to, not subscribing again",
                    get_type_name(message_cls),
                )
        self._processes.append(process)

    async def deregister_process(self, process: Process):
        _log.info(
            "Deregistering process %s which handles message type %s",
            process.name,
            len(process.message_classes),
        )

        self._processes.remove(process)
        for message_type in process.message_classes:
            if not any(message_type in p.message_classes for p in self._processes):
                await self.unsubscribe_from(message_type)
            else:
                _log.debug(
                    "Message type %s still handled by another process, not unsubscribing",
                    get_type_name(message_type),
                )

    def _on_connect(self, client, flags, rc, properties):
        _log.info("Successfully connected to MQTT broker with result code %s", rc)

    async def _on_message(self, client, topic, payload: bytes, qos, properties):
        try:
            payload_str = payload.decode("utf-8")
        except UnicodeError:
            _log.error("Received message with invalid UTF-8 payload on topic %s", topic)
            return int(gmqtt.constants.PubRecReasonCode.PAYLOAD_FORMAT_INVALID)

        _log.debug("Received message on topic %s with payload '%s'", topic, payload_str)

        try:
            message = from_json(topic, payload_str)
        except ValueError as exc:
            _log.error(
                "Received message with invalid JSON payload on topic %s: %s",
                topic,
                exc,
            )
            return int(gmqtt.constants.PubRecReasonCode.PAYLOAD_FORMAT_INVALID)
        except LookupError as exc:
            _log.debug("Cannot find a matching message class for topic %s", topic)
            _log.error(
                "Received message with unexpected topic %s",
                topic,
            )
            return int(gmqtt.constants.PubRecReasonCode.TOPIC_NAME_INVALID)

        for process in self._processes:
            # No-op if process does not handle this message type, so
            # we can skip checking that.
            await process.handle_message(message)
        return int(gmqtt.constants.PubRecReasonCode.SUCCESS)

    def _on_disconnect(self, client, packet, exc=None):
        _log.info("Succesfully disconnected from MQTT broker")

    def _on_subscribe(self, client, mid, qos, properties):
        _log.info("Successfully subscribed")
