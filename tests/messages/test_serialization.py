import meqtt
import pytest
from meqtt import messages


def remove_whitespace(s):
    """Rmeoves all whitespace from a string."""

    return "".join(c for c in s if not c.isspace())


@pytest.fixture(autouse=True)
def clear_message_classes():
    messages.clear_message_classes()


def test_serialization_identity():
    @meqtt.message("test/topic")
    class ExampleMessage(meqtt.Message):
        value: int

    msg = ExampleMessage(42)

    topic, data = messages.to_json(msg)
    assert topic == "test/topic"
    assert remove_whitespace(data) == '{"value":42}'
    parsed_msgs = list(messages.from_json("test/topic", data))
    assert len(parsed_msgs) == 1
    parsed_msg = parsed_msgs[0]
    assert isinstance(parsed_msg, ExampleMessage)
    assert parsed_msg.topic == "test/topic"
    assert parsed_msg.value == 42


def test_on_external_data():
    data = '{ "r": 254, "g": 21, "b": 100 }'

    @meqtt.state("test/topic")
    class ExampleMessage(meqtt.Message):
        r: int
        g: int
        b: int

    @meqtt.message("test/topic2")
    class UnrelatedMessage(meqtt.Message):
        value: int

    ExampleMessage(r=254, g=21, b=100)
    msgs = list(messages.from_json("test/topic", data))
    assert len(msgs) == 1
    msg = msgs[0]
    assert isinstance(msg, ExampleMessage)
    assert msg.topic == "test/topic"
    assert msg.r == 254
    assert msg.g == 21
    assert msg.b == 100


def test_topic_variables():
    @meqtt.message("test/{id:d}/topic/{name}")
    class ExampleMessage(meqtt.Message):
        id: int
        name: str
        value: int

    # test serialization
    msg = ExampleMessage(42, "test", 21)
    topic, data = messages.to_json(msg)
    assert topic == "test/42/topic/test"
    assert remove_whitespace(data) == '{"value":21}'

    # test deserialization
    topic = "test/1337/topic/other_value"
    data = '{ "value": 123456 }'
    msgs = list(messages.from_json(topic, data))
    assert len(msgs) == 1
    msg = msgs[0]
    assert isinstance(msg, ExampleMessage)
    assert msg.topic == topic
    assert msg.id == 1337
    assert msg.name == "other_value"
    assert msg.value == 123456

    # test invalid topic
    topic = "test/not_a_number/topic/test"
    data = '{ "value": 123456 }'
    msgs = list(messages.from_json(topic, data))
    assert len(msgs) == 0


def test_not_serializable():
    class NotSerializable:
        pass

    @meqtt.message("test/topic")
    class ExampleMessage(meqtt.Message):
        value: NotSerializable

    msg = ExampleMessage(NotSerializable())
    with pytest.raises(ValueError):
        messages.to_json(msg)


def test_invalid_json():
    @meqtt.state("test/topic")
    class ExampleMessage(meqtt.Message):
        value: int

    with pytest.raises(ValueError):
        list(messages.from_json("test/topic", "not json"))
