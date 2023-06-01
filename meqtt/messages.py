"""Classes, function and decorators related to message object."""

import dataclasses
import inspect
import json
import logging
import re
from abc import ABC
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, Tuple, Type, dataclass_transform

import parse

logger = logging.getLogger(__name__)

_message_classes = []


class MessageType(Enum):
    """The type of a message.

    Attributes:
        MESSAGE: A general message.
        STATE: A state message.
    """

    MESSAGE = "message"
    STATE = "state"


# Someday, I'd like this to be a Protocol, so that the user code does not have
# to inherit from Message explicitly, removing the duplication of using a
# decorator and a base class.  But currently, there does not seem to be a way to
# hint that the message() decorator implements the protocol.  So this is the
# only way I could find to make mypy happy.
class Message(ABC):
    """Base class for all message object.

    Inheriting classes are also assumed to be dataclasses.

    Attributes:
        topic_pattern: Describes the topic(s) this class maps to.  It is a
            format string with zeor or more placeholders.
        message_type: The type of the message.
        _topic_parser: The parser used to parse the topic (interal).
    """

    topic_pattern: str
    message_type: MessageType
    _topic_parser: parse.Parser

    @property
    def topic(self) -> str:
        """The topic this specific instance maps to.

        This only applies to the object itself, not to the class.  The topic is
        always a single topic while :py:attr:`topic_pattern` can contain
        placeholders and wildcards.
        """

        return self.topic_pattern.format_map(dataclasses.asdict(self))

    @classmethod
    @property
    def topic_mask(cls) -> str:
        """A MQTT topic mask that matches at least all topics this class maps to.

        The topic pattern can be more specific than the topic mask.  For example,
        the topic pattern ``foo/{bar:d}`` has the topic mask ``foo/+``.  The
        mask matches any string in this example while the pattern only matches
        numbers in the placeholder segment.
        """

        split_path = cls.topic_pattern.split("/")
        # replace formatting specifiers with a wildcard
        topic_filter = [
            # match text enclosed in single curly braces
            p if not re.search(r"(^|[^\{])\{[^}]*\}([^\}]|$)", p) else "+"
            for p in split_path
        ]
        return "/".join(topic_filter)


def _get_message_variable(message) -> Dict[str, Any]:
    """Returns the members that are not part of the topic as a dict.

    Variables that are part of the topic are not sent as part of the message
    body as this would be duplication.  This function takes a message object and
    returns its members that are not part of the topic as a dict.

    Parameters:
        message: The message object to get the variables from.
    Returns:
        A dict with the variables that are not part of the topic.
    """

    named_fields = message._topic_parser.named_fields
    return {
        name: value
        for name, value in dataclasses.asdict(message).items()
        if name not in named_fields
    }


def to_json(message: Message) -> Tuple[str, str]:
    """Serialize a message object to a JSON string.

    Also generate the specific topic for this message object.

    Parameters:
        message: A message object.
    Returns:
        A tuple with the topic and the JSON string.
    Raises:
        ValueError: If the message could not be converted to JSON.
    """

    message_variables = _get_message_variable(message)

    # TODO: Handle members with non-trivial types
    try:
        return message.topic, json.dumps(message_variables)
    except TypeError as exc:
        raise ValueError(
            f"Message {message} could not be converted to JSON: {exc}"
        ) from exc


def from_json(topic: str, input: str) -> Iterable[Message]:
    """Deserialize a JSON string to zero or more message objects.

    This may return multiple messages if topic patterns overlap.

    Parameters:
        topic: The topic the message was received on.
        input: The JSON string.
    Returns:
        An iterable of message objects that map to the topic and where a
        deserialization was successful.
    """
    # TODO: Handle non-trivial types
    # TODO: Error handling

    # Read the message contents
    try:
        message = json.loads(input)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON: {exc}") from exc

    for cls in _message_classes:
        # Check if the topic matches the pattern.
        result = cls._topic_parser.parse(topic)
        if result is None:
            continue

        # Extract variables from the topic.
        topic_variables = result.named

        arguments = topic_variables | message
        argument_names = set(arguments.keys())

        # Get all keyword-arguments of the constructur to see if they match the
        # topic variables.
        parameter_names = set(inspect.signature(cls).parameters.keys())

        additional_args = argument_names - parameter_names
        if additional_args:
            error = f"Message {cls.__name__} does not contain the following fields that appear in a received message on topic {topic}: {','.join(additional_args)}"
            logger.warning(error)

        missing_args = parameter_names - argument_names
        if missing_args:
            error = f"Message {cls.__name__} requires the following fields that are not present in a received message on topic {topic}: {','.join(missing_args)}"
            logger.warning(error)
            raise ValueError(error)
        yield cls(
            **{
                name: value
                for name, value in arguments.items()
                if name in parameter_names
            }
        )


# Interestingly, this decorator has to be applied to the function returning the
# actual decorator.
@dataclass_transform()
def message(topic_pattern: str):
    """A decorator that turns a class into a message object class.

    The resulting class will also become a dataclass.

    The topic may contain named formatting specifications like
    ``"test/{id}/value"`` which will be mapped to members of the class with the
    same name.  Positional formatting specifiers are currently not supported.
    The formatting specifier has to yield the same type as the corresponding
    member's type annotation.

    For example::

        @message("test/{id:d}/field/{name}")

    will require a field of type ``int`` named ``id`` and a field of type
    ``str`` named ``name`` to be present in the class.

    MQTT wildcards like ``+`` and ``#`` are not allowed in the topic pattern.
    """

    _check_topic_name(topic_pattern)

    def decorator(cls):
        data_cls = dataclass(cls)
        data_cls.message_type = MessageType.MESSAGE
        data_cls.topic_pattern = topic_pattern
        data_cls._topic_parser = parse.compile(topic_pattern)
        _message_classes.append(data_cls)
        assert is_message_cls(data_cls)
        return data_cls

    return decorator


@dataclass_transform()
def state(topic_pattern: str):
    """A decorator that turns a class into a message object class.

    Like :py:func:`message`, but with a slightly different handling on the MQTT
    side.  Most notably, the retain flag is set for messages of this type.
    """

    _check_topic_name(topic_pattern)

    def decorator(cls):
        cls = message(topic_pattern)(cls)
        cls.message_type = MessageType.STATE
        return cls

    return decorator


def clear_message_classes():
    """Remove all registered message classes.

    This is mainly intended for clearing locally defined messages in unit tests.
    """

    _message_classes.clear()


def is_message_obj(obj: Any) -> bool:
    """Returns True if the object can be used as a message."""

    return is_message_cls(type(obj))


def is_message_cls(cls: Type) -> bool:
    """Returns True if the class can be used as a message."""

    return issubclass(cls, Message) and dataclasses.is_dataclass(cls)


def _check_topic_name(topic: str):
    """Returns True if the topic is valid for any registered message.

    See 4.7.3 Topic semantic and usage in the MQTT 5.0 specification for
    details.
    """

    segments = topic.split("/")

    # Topics may not be empty
    if len(topic) == 0:
        raise ValueError("Empty topic")

    # Check if any segment but the first or last is empty
    if any(len(s) == 0 for s in segments[1:-1]):
        raise ValueError("Empty topic segment")

    # Check if any segment contains a wildcard
    if "+" in topic or "#" in topic:
        raise ValueError("Wildcards are not allowed in topic names")

    # Check if the topic contains an unicode U+0000 character
    if "\u0000" in topic:
        raise ValueError("Topic contains an U+0000 character")
