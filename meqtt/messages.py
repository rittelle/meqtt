import abc
import dataclasses
import json

from abc import ABC, abstractmethod
from typing import Dict

_message_classes = {}

class Message(ABC):
    """Base class for all message object.

    Inheriting classes are also assumed to be dataclasses.
    """

    def to_json():
        return _to_json(self)

    @staticmethod
    def from_json(string: str):
        return _from_json(string)


def _get_class_name(cls) :
    return f"{cls.__module__}.{cls.__name__}"

def _to_json(message: Message) -> str:
    fields = dataclasses.asdict(message)
    # TODO: Filter out properties
    cls = message.__class__
    msg = {
        'type': _get_class_name(cls),
        'data': fields
    }
    return json.dumps(msg)

def _from_json(input: str) -> Message:
    msg = json.loads(input)
    # TODO: Error handling
    type_ = msg["type"]
    fields = msg['data']
    cls_candidates = { c for n, c in _message_classes if n == type_ }
    if len(cls_candidates) < 1:
        raise LookupError("Message type not found")
    assert len(cls_candidates) == 1, "Ambiguous message type?"
    cls = cls_candidates[0]
    return cls(**fields)

def message(topic):
    """A decorator that turns a class into a message object class.

    The resulting class will also become a dataclass.
    """

    def decorator(cls):
        # cls.to_json = _to_json
        # cls.from_json = _from_json
        # abc.register(Message, cls)
        _message_classes[_get_class_name(cls)] = cls
        return dataclasses.dataclass(cls)

    return decorator
