import meqtt
import pytest
from meqtt import messages


def remove_whitespace(s):
    """Rmeoves all whitespace from a string."""

    return "".join(c for c in s if not c.isspace())


@pytest.fixture(autouse=True)
def clear_message_classes():
    messages.clear_message_classes()


# see the MQTT 5.0 spec, section 4.7.3
@pytest.mark.parametrize(
    "topic",
    (
        "test/topic",
        "test/topic/",
        "/t",
        "test topic/with spaces",
        "/",
        "{/}",
        "test{{/",
    ),
)
def test_normal_names(topic):
    messages.clear_message_classes()

    @meqtt.state(topic)
    class Message(meqtt.Message):
        pass

    assert Message.topic_mask == topic


@pytest.mark.parametrize(
    "topic",
    (
        "",
        "no//empty/segments",
        "//",
        "no \u0000 zero character",
        "no/+/wildcards",
        "or/#/these",
        "see/#",
    ),
)
def test_invalid_topics(topic):
    with pytest.raises(ValueError):

        @meqtt.message(topic)
        class Message(meqtt.Message):
            pass


@pytest.mark.parametrize(
    "topic_mask,topic_pattern",
    (
        ("/{id:d}/tag/name-{name}", "/+/tag/+"),
        ("{var}-suffix/hi", "+/hi"),
        ("{{not-var}}/hi", "{{not-var}}/hi"),
    ),
)
def test_topic_variables(topic_mask, topic_pattern):
    @meqtt.message(topic_mask)
    class Message(meqtt.Message):
        id: int
        name: str

    assert Message.topic_mask == topic_pattern
