from dataclasses import dataclass
import logging
from typing import AsyncContextManager, List, Optional, Set, Type

import gmqtt

from meqtt.messages import Message, from_json, to_json
from meqtt.processes import Process
from meqtt.utils import get_type_name

_log = logging.getLogger(__name__)


@dataclass
class ConnectionInfo:
    """Where and how to connect to the MQTT broker."""

    host: str
    port: int = 1883
    use_ssl: bool = False
    username: Optional[str] = None
    password: Optional[str] = None

    def __post_init__(self):
        if self.username is None and self.password is not None:
            raise ValueError(
                "A password can only be set when a username is set as well"
            )


class Connection(AsyncContextManager):
    def __init__(self, connection_info: ConnectionInfo, client_id: str):
        self._client = gmqtt.Client(client_id)
        self._connection_info = connection_info

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
        _log.info(
            "Connecting to MQTT broker on %s on port %d",
            self._connection_info.host,
            self._connection_info.port,
        )
        if self._connection_info.username is not None:
            if self._connection_info.password is None:
                _log.info("Logging in with username only")
            else:
                _log.info("Logging in with username and password")
            self._client.set_auth_credentials(
                self._connection_info.username, self._connection_info.password
            )
        await self._client.connect(
            host=self._connection_info.host,
            port=self._connection_info.port,
            ssl=self._connection_info.use_ssl,
        )

    async def disconnect(self):
        _log.info("Disconnecting from MQTT broker")
        await self._client.disconnect()

    async def subscribe_to(self, message_cls: Type[Message]):
        topic = message_cls.topic_mask
        _log.debug(
            'Message type %s matches to topic "%s"',
            get_type_name(message_cls),
            topic,
        )
        _log.info("Subscribing to topic %s", topic)
        self._client.subscribe(topic, qos=2)

    async def unsubscribe_from(self, message_cls: Type[Message]):
        topic = message_cls.topic_mask
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
        message_classes = list(process.handled_message_classes)
        _log.info(
            "Registering process %s which handles %d message types",
            process.name,
            len(message_classes),
        )
        self._processes.append(process)
        for message_cls in message_classes:
            await self.add_process_subscription(process, message_cls)

    async def deregister_process(self, process: Process):
        message_classes = list(process.handled_message_classes)
        _log.info(
            "Deregistering process %s which handles message type %s",
            process.name,
            len(message_classes),
        )

        self._processes.remove(process)
        for message_type in message_classes:
            if not self._is_message_type_subscription_required(message_type):
                await self.unsubscribe_from(message_type)

    async def add_process_subscription(
        self, process: Process, message_cls: Type[Message]
    ):
        if process not in self._processes:
            raise ValueError(
                f"Process {process.name} is not registered with this connection"
            )
        assert (
            message_cls in process.handled_message_classes
        ), "The process needs to update its list of handled message types first"
        if not self._is_message_type_subscription_required(
            message_cls, excluded_processes={process}
        ):
            _log.info(
                'Subscribing to message type "%s" due to process "%s"',
                get_type_name(message_cls),
                process.name,
            )
            await self.subscribe_to(message_cls)
        else:
            _log.debug(
                "Message type %s already is subscribed to, not subscribing again",
                get_type_name(message_cls),
            )

    async def remove_process_subscription(
        self, process: Process, message_cls: Type[Message]
    ):
        if process not in self._processes:
            raise ValueError(
                f"Process {process.name} is not registered with this connection"
            )
        assert (
            message_cls not in process.handled_message_classes
        ), "The process needs to update its list of handled message types first"
        if not self._is_message_type_subscription_required(
            message_cls, excluded_processes={process}
        ):
            _log.info(
                'Unsubscribing from message type "%s" as process "%s" does not require it anymore',
                get_type_name(message_cls),
                process.name,
            )
            await self.unsubscribe_from(message_cls)
        else:
            _log.debug(
                "Message type %s still handled by another process, not unsubscribing",
                get_type_name(message_cls),
            )

    def _is_message_type_subscription_required(
        self, message_cls: Type[Message], excluded_processes: Set[Process] = set()
    ):
        return any(
            message_cls in p.handled_message_classes
            for p in self._processes
            if p not in excluded_processes
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
            messages = list(from_json(topic, payload_str))
        except ValueError as exc:
            _log.error(
                "Received message with invalid JSON payload on topic %s: %s",
                topic,
                exc,
            )
            return int(gmqtt.constants.PubRecReasonCode.PAYLOAD_FORMAT_INVALID)

        if not messages:
            _log.debug("Cannot find a matching message class for topic %s", topic)
            _log.error(
                "Received message with unexpected topic %s",
                topic,
            )
            return int(gmqtt.constants.PubRecReasonCode.TOPIC_NAME_INVALID)

        message_handled = False
        for message in messages:
            for process in self._processes:
                # No-op if process does not handle this message type, so
                # we can skip checking that.
                message_handled |= await process.handle_message(message)
        if not message_handled:
            _log.warning(
                'A message on topic "%s" was not handled by any process', topic
            )
        return int(gmqtt.constants.PubRecReasonCode.SUCCESS)

    def _on_disconnect(self, client, packet, exc=None):
        _log.info("Succesfully disconnected from MQTT broker")

    def _on_subscribe(self, client, mid, qos, properties):
        _log.info("Successfully subscribed")
