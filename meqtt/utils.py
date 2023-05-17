from typing import Type


def get_type_name(type_: Type) -> str:
    """Returns the name of the type with the full module name."""

    return f"{type_.__module__}.{type_.__name__}"
