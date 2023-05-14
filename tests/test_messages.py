import meqtt
from meqtt import messages
from meqtt.messages import Message

def test_instantiation():
    @meqtt.message("/test/topic")
    class ExampleMessage:
        value: int

    msg = ExampleMessage(42)

    msg2 = messages.from_json("/test/topic", messages.to_json(msg))
    assert msg2.topic == "/test/topic"
    assert msg2.value == 42

def test_on_external_data():
    data = '{ "r": 254, "g": 21, "b": 100 }'

    @meqtt.message("/test/topic")
    class ExampleMessage(meqtt.Message):
        r: int
        g: int
        b: int

    msg = messages.from_json("/test/topic", data)
    assert msg.topic == "/test/topic"
    assert msg.r == 254
    assert msg.g == 21
    assert msg.b == 100
