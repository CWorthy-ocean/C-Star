import asyncio
import sys
import typing as t
from argparse import Namespace


async def invoke(args: Namespace) -> None:
    """Invoke the command handler registered with the parser.

    Parameters
    ----------
    args : Namespace
        Arguments parsed from the CLI.
    """
    handler: t.Callable[[Namespace], None] = args.handler
    handler(args)


def main() -> None:
    """Parse arguments passed to the CLI and trigger the associated request handlers."""
    args = sys.argv[1:]

    import cstar.cli.blueprint  # noqa: F401
    import cstar.cli.template  # noqa: F401
    import cstar.cli.workplan  # noqa: F401
    from cstar.cli.core import main_parser

    ns = main_parser.parse_args(args)
    try:
        asyncio.run(invoke(ns))
    except Exception as ex:
        print(f"{ex}")


if __name__ == "__main__":
    main()
