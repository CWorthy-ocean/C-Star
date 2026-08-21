import logging
from collections import Counter
from importlib.metadata import entry_points

import typer

from cstar.applications import *  # noqa: F403
from cstar.cli.admin import app as app_admin
from cstar.cli.blueprint import app as app_blueprint
from cstar.cli.cache import app as app_cache
from cstar.cli.common import common_callback
from cstar.cli.environment import app as app_env
from cstar.cli.template import app as app_template
from cstar.cli.workplan import app as app_workplan

CLI_PLUGIN_GROUP = "cstar.cli"
"""Entry-point group third-party packages use to add `cstar <name> ...` subcommands."""

logger = logging.getLogger(__name__)


def attach_subcommands(app: typer.Typer) -> None:
    """Attach subcommands dynamically to the main typer app and configure
    the command callback to enable shared options.

    Core subcommands are attached first, then any third-party plugins
    discovered via the ``cstar.cli`` entry-point group.
    """
    subcommands: list[tuple[typer.Typer, str]] = [
        (app_blueprint, "blueprint"),
        (app_env, "env"),
        (app_template, "template"),
        (app_workplan, "workplan"),
        (app_admin, "admin"),
        (app_cache, "cache"),
    ]

    try:
        for command_app, command_name in subcommands:
            if command_app.registered_groups or command_app.registered_commands:
                app.add_typer(
                    command_app,
                    name=command_name,
                )
    except Exception as ex:
        print(f"An error occurred while handling request: {ex}")

    attach_plugin_subcommands(app, taken={name for _, name in subcommands})


def attach_plugin_subcommands(app: typer.Typer, taken: set[str]) -> None:
    """Discover and attach third-party ``cstar.cli`` entry-point plugins.

    Never raises: a misbehaving plugin is skipped with a warning so it cannot
    break the core CLI, and a plugin whose name collides with a core
    subcommand (or an earlier plugin) is skipped so core commands cannot be
    shadowed through this path.
    """
    if plugins := set(entry_points(group=CLI_PLUGIN_GROUP)):
        c = Counter[str](name for name in taken)
        c.update(ep.name for ep in plugins)

        if conflicts := {k for k in c if c[k] > 1}:
            logger.warning(
                f"Name conflicts occurred for the plugins: {','.join(conflicts)}"
            )

        loadable = {ep for ep in plugins if ep.name not in conflicts}

        for ep in loadable:
            try:
                plugin_app = ep.load()
            except Exception:
                logger.warning(f"Plugin failed to import: {ep.name}")
            else:
                if isinstance(plugin_app, typer.Typer):
                    app.add_typer(plugin_app, name=ep.name)
                else:
                    logger.warning(
                        f"Ignoring plugin {ep.name!r}: expected typer.Typer, "
                        f"got {type(plugin_app).__name__}"
                    )


app = typer.Typer(
    callback=common_callback,
    help="The C-Star CLI enables command-line management and execution of C-Star workplans and blueprints.",
)
attach_subcommands(app)


def main() -> None:
    """Main entrypoint for the complete C-Star CLI."""
    app()


if __name__ == "__main__":
    main()
