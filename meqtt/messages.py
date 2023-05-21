import dataclasses
import json
from abc import ABC
from dataclasses import dataclass
import re
from typing import Any, Dict, Iterable, Tuple, Type, dataclass_transform
import parse

_message_classes = []


# Someday, I'd like this to be a Protocol, so that the user code does not have
# to inherit from Message explicitly, removing the duplication of using a
# decorator and a base class.  But currently, there does not seem to be a way to
# hint that the message() decorator implements the protocol.  So this is the
# only way I could find to make mypy happy.
class Message(ABC):
    """Base class for all message object.

    Inheriting classes are also assumed to be dataclasses.
    """

    topic_pattern: str
    _topic_parser: parse.Parser

    @property
    def topic(self) -> str:
        return self.topic_pattern.format_map(dataclasses.asdict(self))

    @classmethod
    @property
    def topic_mask(cls) -> str:
        split_path = cls.topic_pattern.split("/")
        # replace formatting specifiers with a wildcard
        topic_filter = [
            # match text enclosed in single curly braces
            p if not re.search(r"(^|[^\{])\{[^}]*\}([^\}]|$)", p) else "+"
            for p in split_path
        ]
        return "/".join(topic_filter)


def _get_message_variable(message) -> Dict[str, Any]:
    """Returns the members that are not part of the topic as a dict."""

    named_fields = message._topic_parser.named_fields
    return {
        name: value
        for name, value in dataclasses.asdict(message).items()
        if name not in named_fields
    }


def to_json(message: Message) -> Tuple[str, str]:
    message_variables = _get_message_variable(message)

    # TODO: Handle members with non-trivial types
    return message.topic, json.dumps(message_variables)


def from_json(topic: str, input: str) -> Iterable[Message]:
    """This may return multiple messages if topic patterns overlap."""
    # TODO: Handle non-trivial types
    # TODO: Error handling
    for cls in _message_classes:
        # Check if the topic matches the pattern.
        if (result := cls._topic_parser.parse(topic)) is None:
            continue

        # Extract variables from the topic.
        topic_variables = result.named

        # Read the message contents
        try:
            message = json.loads(input)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON: {exc.msg}") from exc

        yield cls(**(topic_variables | message))


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
        data_cls = dataclasses.dataclass(cls)
        data_cls.topic_pattern = topic_pattern
        data_cls._topic_parser = parse.compile(topic_pattern)
        _message_classes.append(data_cls)
        assert is_message_cls(data_cls)
        return data_cls

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
