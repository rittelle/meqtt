import inspect

TYPE_ATTRIBUTE = "_meqtt_type"


def task(method):
    """Dekorator, der eine Methode als Task markiert.

    Tasks müssen asynchron sein.
    """

    if not inspect.iscoroutinefunction(method):
        raise ValueError(f"Tasks have to be async: {method.__name__}")
    setattr(method, TYPE_ATTRIBUTE, "task")
    return method


def handler(method):
    """Dekoator, der eine Methode als Handler markiert."""

    if not inspect.iscoroutinefunction(method):
        raise ValueError(f"Tasks have to be async: {method.__name__}")
    setattr(method, TYPE_ATTRIBUTE, "handler")
    return method
