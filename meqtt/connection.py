import logging
from typing import AsyncContextManager
import gmqtt

from meqtt.messages import Message, from_json, to_json

_log = logging.getLogger(__name__)


class Connection(AsyncContextManager):
    def __init__(self, host, client_id):
        self._client = gmqtt.Client(client_id)
        self._host = host

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

    async def publish(self, message: Message):
        _log.debug("Publishing message on topic %s", message.topic)
        self._client.publish(message.topic, to_json(message), qos=2)  # exactly once

    def _on_connect(self, client, flags, rc, properties):
        _log.info("Successfully connected to MQTT broker with result code %s", rc)

    def _on_message(self, client, topic, payload, qos, properties):
        _log.debug("Received message on topic %s", topic)

    def _on_disconnect(self, client, packet, exc=None):
        _log.info("Succesfully disconnected from MQTT broker")

    def _on_subscribe(self, client, mid, qos, properties):
        _log.info("Successfully subscribed")
