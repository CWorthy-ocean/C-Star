import typer

from cstar.cli.blueprint import app as app_blueprint
from cstar.cli.common import common_callback
from cstar.cli.environment import app as app_env
from cstar.cli.template import app as app_template
from cstar.cli.workplan import app as app_workplan


def attach_subcommands(app: typer.Typer) -> None:
    """Attach subcommands dynamically to the main typer app and configure
    the command callback to enable shared options.
    """
    subcommands: list[tuple[typer.Typer, str]] = [
        (app_blueprint, "blueprint"),
        (app_env, "env"),
        (app_template, "template"),
        (app_workplan, "workplan"),
    ]

    try:
        for command_app, command_name in subcommands:
            if command_app.registered_groups or command_app.registered_commands:
                app.add_typer(
                    command_app,
                    name=command_name,
                    callback=common_callback,
                )
    except Exception as ex:
        print(f"An error occurred while handling request: {ex}")


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
