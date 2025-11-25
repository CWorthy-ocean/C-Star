import asyncio
import sys
from argparse import Namespace

from cstar.cli.core import AsyncHandlerFn


async def invoke(args: Namespace) -> None:
    """Invoke the command handler registered with the parser.

    Parameters
    ----------
    args : Namespace
        Arguments parsed from the CLI.
    """
    handler: AsyncHandlerFn = args.handler
    await handler(args)


def main(args: list[str] | None = None) -> None:
    """Parse arguments passed to the CLI and trigger the associated request handlers.

    Parameters
    ----------
    args : list[str]
        Arguments for triggering a CLI action handler. If not provided, `sys.argv`
        will be used.
    """
    if not args:
        args = sys.argv[1:]

    import cstar.cli.blueprint  # noqa: F401
    import cstar.cli.template  # noqa: F401
    import cstar.cli.workplan  # noqa: F401
    from cstar.cli.core import main_parser

    ns = main_parser.parse_args(args)

    try:
        asyncio.run(invoke(ns))
    except Exception as ex:
        print(f"An error occurred while handling request: {ex}")


if __name__ == "__main__":
    main()
