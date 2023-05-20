import dataclasses
import json
from abc import ABC
from dataclasses import dataclass
from typing import Any, Tuple, Type, dataclass_transform

_message_classes = {}


# Someday, I'd like this to be a Protocol, so that the user code does not have
# to inherit from Message explicitly.  But currently, there does not seem to be
# a way to hint that the message() decorator implements the protocol.  So this
# is the only way I could find to make mypy happy.
class Message(ABC):
    """Base class for all message object.

    Inheriting classes are also assumed to be dataclasses.
    """

    topic: str


def to_json(message: Message) -> Tuple[str, str]:
    fields = dataclasses.asdict(message)
    string = {k: v for k, v in fields.items() if k != "topic"}
    # TODO: Handle members with non-trivial types
    return message.topic, json.dumps(string)


def from_json(topic: str, input: str) -> Message:
    # TODO: Handle non-trivial types
    # TODO: Error handling
    try:
        message = json.loads(input)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON: {exc.msg}") from exc
    try:
        cls = _message_classes[topic]
    except KeyError:
        raise LookupError("Message type not found")
    return cls(**message)


# Interestingly, this decorator has to be applied to the function returning the
# actual decorator.
@dataclass_transform()
def message(topic):
    """A decorator that turns a class into a message object class.

    The resulting class will also become a dataclass.
    """

    def decorator(cls):
        data_cls = dataclasses.dataclass(cls)
        data_cls.topic = topic
        _message_classes[topic] = data_cls
        assert is_message_cls(data_cls)
        return data_cls

    return decorator


@dataclass_transform()
def test_message_decorator(cls):
    return dataclass(cls)


def is_message_obj(obj: Any) -> bool:
    """Returns True if the object can be used as a message."""

    return is_message_cls(type(obj))


def is_message_cls(cls: Type) -> bool:
    """Returns True if the class can be used as a message."""

    return issubclass(cls, Message) and dataclasses.is_dataclass(cls)


dataclasses.dataclass
