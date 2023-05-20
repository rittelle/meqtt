import meqtt
import pytest
from meqtt import messages


def remove_whitespace(s):
    """Rmeoves all whitespace from a string."""

    return "".join(c for c in s if not c.isspace())


@pytest.fixture(autouse=True)
def clear_message_classes():
    messages.clear_message_classes()


def test_normal_names():
    # see the MQTT 5.0 spec, section 4.7.3
    topics = (
        "test/topic",
        "test/topic/",
        "/t",
        "test topic/with spaces",
        "/",
        "{/}",
        "test{{/",
    )
    for name in topics:
        messages.clear_message_classes()

        @meqtt.message(name)
        class Message(meqtt.Message):
            pass

        assert Message.topic_mask == name


def test_topic_variables():
    @meqtt.message("/{id:d}/tag/name-{name}")
    class Message1(meqtt.Message):
        id: int
        name: str

    assert Message1.topic_mask == "/+/tag/+"

    @meqtt.message("{var}-suffix/hi")
    class Message2(meqtt.Message):
        id: int
        name: str

    assert Message2.topic_mask == "+/hi"

    @meqtt.message("{{not-var}}/hi")
    class Message3(meqtt.Message):
        id: int
        name: str

    assert Message3.topic_mask == "{{not-var}}/hi"
