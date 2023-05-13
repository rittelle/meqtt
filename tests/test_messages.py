import meqtt

def test_instantiation():
    @meqtt.message("/test/topic")
    class ExampleMessage(meqtt.Message):
        value: int

    msg = ExampleMessage(42)
    print()
