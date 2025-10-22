import os
import typing as t
from argparse import Action, ArgumentParser, Namespace, _SubParsersAction
from collections import defaultdict
from pathlib import Path

ParseItemKey: t.TypeAlias = tuple[str, str | None]
"""Composite key uniquely identifying an action."""

CommandTree: t.TypeAlias = dict[str, t.Any | None]
"""The dictionary containing lookups to registered commands."""

ActionTree: t.TypeAlias = dict[ParseItemKey, list[t.Callable]]
"""The dictionary containing lookups to registered actions."""

ActionResult: t.TypeAlias = _SubParsersAction | ArgumentParser
"""The outputs of an action registration function."""

ActionFn: t.TypeAlias = t.Callable[[_SubParsersAction], ActionResult]
"""A function that adds a command or action into the CLI."""

RegistryResult: t.TypeAlias = tuple[ParseItemKey, ActionFn]
"""The output of a registry function."""

RegistryFn: t.TypeAlias = t.Callable[[], RegistryResult]
"""A function that registers a command or action into the CLI."""


_command_tree: CommandTree = defaultdict(lambda: None)
_action_tree: ActionTree = defaultdict(lambda: [])
_action_map: dict[str, list[ActionFn]] = {}


def build_parser() -> tuple[ArgumentParser, _SubParsersAction]:
    """Configure the CLI argument parser.

    Returns
    -------
    tuple[ArgumentParser, _SubParsersAction]
        Tuple containing the main parser and the subparser for commands.
    """
    parser = ArgumentParser("cstar")

    subparsers = parser.add_subparsers(
        title="command",
        help="Available commands",
        required=True,
        dest="command",
    )

    interactive: bool = os.getenv("CSTAR_INTERACTIVE", "1") == "1"
    parser.set_defaults(interactive=interactive)

    return parser, subparsers


main_parser, subparsers = build_parser()


def cli_activity(func: RegistryFn) -> RegistryFn:
    """Decorate functions that produce CLI behaviors to automatically register them.

    Parameters
    ----------
    func : RegistryFn
        Function to automatically register as a CLI action.

    Returns
    -------
    RegistryFn
        The original CLI registry function
    """
    a_tree = _action_tree
    c_tree = _command_tree

    def register() -> RegistryResult:
        """Registers a function with the global CLI handler registry.

        Returns
        -------
        RegistryResult
        """
        (cmd, action), parser_fn = func()

        if cmd and action:
            a_tree[(cmd, action)].append(parser_fn)
        elif cmd and not action:
            c_tree[cmd] = parser_fn(subparsers)

        # look for any new menu items to build
        for (c_, a_), fn_list in a_tree.items():
            k = f"{c_}.{a_}"
            if k in _action_map or c_ not in c_tree:
                continue

            for action_fn in fn_list:
                cp = c_tree[c_]
                action_fn(cp)

            a_tree[(c_, a_)].clear()

        return (cmd, action), parser_fn

    register()

    return func


class PathConverterAction(Action):
    """Custom parser action that converts a standard path attribute to a pathlib.Path."""

    def __call__(
        self,
        parser: ArgumentParser,
        namespace: Namespace,
        values: str | t.Sequence[str] | None,
        option_string=None,
    ) -> None:
        """Convert the value supplied to the parameter into a `pathlib.Path` object.

        See: https://docs.python.org/3/library/argparse.html#action-classes
        """
        if not values or not isinstance(values, str):
            setattr(namespace, self.dest, values)
            return

        path = Path(values)
        setattr(namespace, self.dest, path)
