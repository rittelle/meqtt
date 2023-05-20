from contextlib import AsyncExitStack
from typing import (
    AsyncContextManager,
    Awaitable,
    Callable,
    Iterable,
    ParamSpec,
    Type,
)


def get_type_name(type_: Type) -> str:
    """Returns the name of the type with the full module name."""

    return f"{type_.__module__}.{type_.__name__}"


P = ParamSpec("P")


async def call_with_async_context_managers(
    context_managers: Iterable[AsyncContextManager],
    f: Callable[P, Awaitable[None]],
    *args: P.args,
    **kwargs: P.kwargs,
) -> None:
    """Call a function with async context managers.

    The context managers are entered in the order they are provided.  If an
    exception occurs, the context managers are exited in the reverse order.

    Take the following example:

        await call_with_context_managers((cm1, cm2), f, 42)

    The above is equivalent to:

        async with context_manager1, context_manager2:
            await f(42)
    """

    async with AsyncExitStack() as stack:
        for context_manager in context_managers:
            await stack.enter_async_context(context_manager)
        await f(*args, **kwargs)
