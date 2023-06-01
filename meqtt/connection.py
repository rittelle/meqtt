"""Functionality related to the broker connection.

Mainly, this module contains the :py:class:`Connection` class which handles the
backend connectino.
"""

from dataclasses import dataclass
import logging
from typing import AsyncContextManager, List, Optional, Set, Type

import gmqtt

from meqtt.messages import Message, MessageType, from_json, to_json
from meqtt.processes import Process
from meqtt.utils import get_type_name

_log = logging.getLogger(__name__)


@dataclass
class ConnectionInfo:
    """Where and how to connect to the MQTT broker.

    Attributes:
        host: The hostname or IP address of the broker.
        port: The port to connect to.
        use_ssl: Whether to use SSL/TLS.
        username: The username to use for authentication, optional.
        password: The password to use for authentication, optional.  Can only be
            set if a username is set as well.
    """

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
    """A connection to the MQTT broker.

    This class is an asynchronous context manager.  It can be used with the
    ``async with`` to ensure that the connection is closed properly even if an
    exception is thrown during its lifetime.
    """

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
        """Connect to the broker."""

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
        """Disconnect from the broker."""

        _log.info("Disconnecting from MQTT broker")
        await self._client.disconnect()

    async def subscribe_to(self, message_cls: Type[Message]):
        """Subscribe a message type.

        The client will subscribe to at least the topics that the given message
        type maps to.  See :py:meth:`Message.topic_mask` for an explanation why
        the subscription may include more topics than needed.

        Parameters:
            message_cls: The message type to subscribe to.
        """

        topic = message_cls.topic_mask
        _log.debug(
            'Message type %s matches to topic "%s"',
            get_type_name(message_cls),
            topic,
        )
        _log.info("Subscribing to topic %s", topic)
        self._client.subscribe(topic, qos=2)

    async def unsubscribe_from(self, message_cls: Type[Message]):
        """Unsubscribe a message type.

        Inverse to :py:meth:`subscribe_to`.

        Parameters:
            message_cls: The message type to unsubscribe from.
        """

        topic = message_cls.topic_mask
        _log.debug(
            'Message type %s matches to topic "%s"',
            get_type_name(message_cls),
            topic,
        )
        _log.info("Unsubscribing from topic %s", topic)
        self._client.unsubscribe(topic)

    async def publish(self, message: Message):
        """Publish a message on the Broker.

        Other connected clients that are subscribed to the topic to the same
        message type should receive a copy of the message.

        Parameters:
            message: The message to publish.
        """

        topic, payload = to_json(message)
        _log.debug('Publishing message on topic "%s" with payload %s', topic, payload)
        match message.message_type:
            case MessageType.MESSAGE:
                # exactly once
                self._client.publish(topic, payload, qos=2)
            case MessageType.STATE:
                # exactly once and retain
                self._client.publish(topic, payload, retain=True, qos=2)
            case _:
                raise ValueError(f"Unknown message type {message.message_type}")

    async def register_process(self, process: Process):
        """Register a process with this connection.

        The process will be registered as a handler for all message types that
        it handles.  Messages published by the process are sent over this
        connection.

        Parameters:
            process: The process to register.
        """

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
        """Deregister a process from this connection.

        The inverse to :py:meth:`register_process`.

        Parameters:
            process: The process to deregister.
        """

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
        """Add a subscription for a message type.

        Subscribe to the topic that the message type maps to if this is not
        already the case.

        Parameters:
            process: The process that this subscription is for.
            message_cls: The message type to subscribe to.
        """

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
        """Remove a subscription for a message type.

        The inverse to :py:meth:`add_process_subscription`.  If the message type
        is still required by another process, it will not be unsubscribed from.

        Parameters:
            process: The process that this subscription is for.
            message_cls: The message type to unsubscribe from.
        """

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
        """Check if a subscription for a message type is required.

        Required means in this case that a new subscription is required to
        listen to the given message type.

        Returns:
            True if a subscription for the message type is required, False
            otherwise.
        """

        return any(
            message_cls in p.handled_message_classes
            for p in self._processes
            if p not in excluded_processes
        )

    def _on_connect(self, client, flags, rc, properties):
        """Callback for when the client connects to the broker."""

        _log.info("Successfully connected to MQTT broker with result code %s", rc)

    async def _on_message(self, client, topic, payload: bytes, qos, properties):
        """Callback for when the client receives a message from the broker."""

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
        """Callback for when the client disconnects from the broker."""

        _log.info("Succesfully disconnected from MQTT broker")

    def _on_subscribe(self, client, mid, qos, properties):
        """Callback for when the client has subscribed to a topic."""

        _log.info("Successfully subscribed")
