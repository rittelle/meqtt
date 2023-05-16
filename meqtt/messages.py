import dataclasses
import json
from abc import ABC

_message_classes = {}


class Message(ABC):
    """Base class for all message object.

    Inheriting classes are also assumed to be dataclasses.
    """

    topic: str


def to_json(message: Message) -> str:
    fields = dataclasses.asdict(message)
    string = {k: v for k, v in fields.items() if k != "topic"}
    # TODO: Handle non-trivial types
    return json.dumps(string)


def from_json(topic: str, input: str) -> Message:
    # TODO: Handle non-trivial types
    # TODO: Error handling
    message = json.loads(input)
    try:
        cls = _message_classes[topic]
    except KeyError:
        raise LookupError("Message type not found")
    return cls(**message)


def message(topic):
    """A decorator that turns a class into a message object class.

    The resulting class will also become a dataclass.
    """

    def decorator(cls):
        # cls.to_json = _to_json
        # cls.from_json = _from_json
        Message.register(cls)
        cls.topic = topic
        _message_classes[topic] = cls
        return dataclasses.dataclass(cls)

    return decorator
